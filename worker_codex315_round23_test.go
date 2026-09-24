package tasuki

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// round23FailBackend fails every ExtendLease while counting calls, so
// renewal-abandonment tests observe the retry budget exactly.
type round23FailBackend struct {
	backend.Backend
	calls atomic.Int32
}

func (b *round23FailBackend) ExtendLease(context.Context, backend.Task, time.Duration) error {
	b.calls.Add(1)
	return errors.New("injected renewal failure")
}

func round23TestWorker(store backend.Backend, lease time.Duration) *Worker {
	return NewWorker(store, WorkerOptions{
		LeaseDuration: lease,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// TestWorker_Round23_RetryDeadlineUsesClaimTime is the regression test for
// round-23 P2a (base the retry deadline on the actual claim time). A slow
// ClaimTasks means the store lease started well before the renewal loop
// runs: the loop must bound its retries from the pre-claim instant, not
// from loop entry.
//
// Layout: 600ms lease, claimBase 600ms in the past (the slow claim), store
// down. The abandonment deadline (base + lease - margin = base + 450ms)
// already passed, so the first failed half-lease tick abandons immediately:
// exactly one ExtendLease (the tick itself) and a lease-loss signal.
// Without the fix the loop measures from entry and burns retries for
// another ~450ms — two or more ExtendLease calls — while a peer may
// already own the lease.
func TestWorker_Round23_RetryDeadlineUsesClaimTime(t *testing.T) {
	ctx := context.Background()
	const lease = 600 * time.Millisecond
	store := &round23FailBackend{}
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		// The claim itself took ~600ms to return (slow store scan); the
		// loop starts now.
		w.extendLeaseLoop(ctx, backend.Task{ID: 7}, done, func() { lost.Add(1) }, time.Now().Add(-600*time.Millisecond))
	}()
	select {
	case <-loopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("renewal loop did not abandon a stale claim")
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1 (stale claim must abandon, not retry)", n)
	}
	if n := store.calls.Load(); n != 1 {
		t.Fatalf("ExtendLease calls = %d, want 1 (the failed tick only; no post-deadline retries)", n)
	}
}

// TestWorker_Round23_RetrySleepsCappedAtAbandonment is the regression test
// for round-23 P2b (cap retry sleeps at the abandonment deadline). An
// uncapped backoff overshoots lastSuccess+lease-margin, so the turn keeps
// running side effects past the abandonment point — and, for short leases,
// past the store expiry itself.
//
// Layout: 500ms lease (margin 125ms, deadline lastSuccess+375ms), store
// down. Backoffs 100/200/400ms: the 400ms sleep must be cut to the ~75ms
// remaining, so the loop attempts at ~375ms, fails, and abandons right at
// the deadline with 3 attempts. Without the fix the loop sleeps the full
// 400ms and abandons at ~700ms — past the 500ms store expiry, where a
// peer may already execute concurrently.
//
// The capped sleep still ends in an attempt (not an early return): a
// retry that lands in-window must be able to recover the turn —
// abandoning without trying would regress the round-22 short-lease
// recovery invariant. The cap only moves the attempt forward to the
// deadline instead of past it.
func TestWorker_Round23_RetrySleepsCappedAtAbandonment(t *testing.T) {
	ctx := context.Background()
	const lease = 500 * time.Millisecond
	store := &round23FailBackend{}
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	start := time.Now()
	_, renewed := w.retryRenewal(ctx, backend.Task{ID: 7}, done, lease, start, func() { lost.Add(1) })
	elapsed := time.Since(start)
	if renewed {
		t.Fatal("renewed = true, want false (store down for the whole window must abandon)")
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1", n)
	}
	if n := store.calls.Load(); n != 3 {
		t.Fatalf("ExtendLease calls = %d, want 3 (100ms + 200ms + deadline attempts; the capped sleep moves the last attempt forward to the deadline instead of past it)", n)
	}
	// Deadline at 375ms; the store lease itself expires at 500ms. The
	// uncapped loop abandons at ~700ms. 600ms splits the two with margin
	// for timer/scheduling jitter on either side.
	if elapsed >= 600*time.Millisecond {
		t.Fatalf("abandoned after %v, want well before 600ms (sleep must stop at the ~375ms abandonment deadline, not overshoot past the 500ms lease)", elapsed)
	}
	if elapsed < 300*time.Millisecond {
		t.Fatalf("abandoned after %v, want >= 300ms (the 100ms + 200ms + capped sleeps must still run; an instant return would skip in-window retries)", elapsed)
	}
}

// TestWorker_Round23_LeaseBaseFromClaim pins the conservative baseline:
// the earlier of the pre-claim wall instant and the claim's
// VisibleAt-derived store start wins, so the deadline never stretches
// past the actual store lease. Zero inputs fall back safely.
func TestWorker_Round23_LeaseBaseFromClaim(t *testing.T) {
	const lease = time.Minute
	now := time.Now()
	// Slow claim: store stamped during the call, after the pre-claim
	// instant. The pre-claim instant (earlier) wins.
	storeStart := now.Add(-time.Second)
	task := backend.Task{VisibleAt: storeStart.Add(lease)}
	if got := leaseBaseFromClaim(task, now.Add(-2*time.Second), lease); !got.Equal(now.Add(-2 * time.Second)) {
		t.Fatalf("slow claim base = %v, want the earlier pre-claim instant %v", got, now.Add(-2*time.Second))
	}
	// Wall clock jumped forward: the VisibleAt-derived start is earlier
	// and wins.
	if got := leaseBaseFromClaim(task, now.Add(time.Hour), lease); !got.Equal(storeStart) {
		t.Fatalf("jumped clock base = %v, want the earlier store start %v", got, storeStart)
	}
	// No VisibleAt (backends that do not report it): pre-claim instant.
	if got := leaseBaseFromClaim(backend.Task{}, now, lease); !got.Equal(now) {
		t.Fatalf("no-VisibleAt base = %v, want %v", got, now)
	}
	// Zero claimBase (older call sites): falls back to ~now, never zero.
	if got := leaseBaseFromClaim(backend.Task{}, time.Time{}, lease); got.IsZero() || time.Since(got) > time.Minute {
		t.Fatalf("zero claimBase = %v, want ~now", got)
	}
}
