package tasuki

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// round22ExtendBackend always succeeds ExtendLease while counting calls, so
// retryRenewal tests observe recovery (the single transient failure is the
// tick failure that entered retryRenewal, modeled by lastSuccess).
type round22ExtendBackend struct {
	backend.Backend
	calls atomic.Int32
}

func (b *round22ExtendBackend) ExtendLease(context.Context, backend.Task, time.Duration) error {
	b.calls.Add(1)
	return nil
}

// TestWorker_Round22_RenewAbandonMarginBelowHalfLease pins the round-22 P2
// invariant: the abandon margin stays strictly below half the lease, so the
// first post-failure check (about half a lease after the last success) can
// still enter the retry loop. Without the fix the 200ms floor put leases at
// or below 400ms at/above half the lease and a single transient failure
// abandoned immediately.
func TestWorker_Round22_RenewAbandonMarginBelowHalfLease(t *testing.T) {
	leases := []time.Duration{
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		2 * time.Second,
		30 * time.Second,
		5 * time.Minute,
	}
	for _, lease := range leases {
		m := renewAbandonMargin(lease)
		if m < 0 {
			t.Errorf("lease %v: margin %v, want >= 0", lease, m)
		}
		if m > 2*time.Second {
			t.Errorf("lease %v: margin %v, want <= 2s", lease, m)
		}
		if m >= lease/2 {
			t.Errorf("lease %v: margin %v, want < half-lease %v (one transient failure must still fit a retry)", lease, m, lease/2)
		}
	}
	// Spot values document the clamp: quarter-lease inside [25ms, 2s].
	spots := map[time.Duration]time.Duration{
		30 * time.Second: 2 * time.Second,
		2 * time.Second:  500 * time.Millisecond,
		300 * time.Millisecond: 75 * time.Millisecond,
		100 * time.Millisecond: 25 * time.Millisecond,
	}
	for lease, want := range spots {
		if got := renewAbandonMargin(lease); got != want {
			t.Errorf("lease %v: margin %v, want %v", lease, got, want)
		}
	}
	// Degenerate leases bottom out at zero instead of going negative.
	for _, lease := range []time.Duration{0, -time.Second} {
		if got := renewAbandonMargin(lease); got != 0 {
			t.Errorf("lease %v: margin %v, want 0", lease, got)
		}
	}
}

// TestWorker_Round22_ShortLeaseTransientFailureRetries is the regression
// test for round-22 P2 (cap the abandonment margin for short leases). A
// 300ms lease with one transient tick failure must recover via an in-window
// retry instead of abandoning: lastSuccess sits half a lease back (the
// failed half-lease tick lands now) and the store is healthy again. Without
// the fix the 200ms floor leaves no window (300ms - 200ms < 150ms elapsed)
// and the turn abandons immediately — needless replay with repeated side
// effects on every transient blip.
func TestWorker_Round22_ShortLeaseTransientFailureRetries(t *testing.T) {
	ctx := context.Background()
	const lease = 300 * time.Millisecond
	store := &round22ExtendBackend{}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: lease,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	// The failed half-lease tick lands now: half a lease since the last
	// success, exactly as extendLeaseLoop enters retryRenewal.
	lastSuccess := time.Now().Add(-lease / 2)
	at, renewed := w.retryRenewal(ctx, backend.Task{ID: 7}, done, lease, lastSuccess, func() {
		lost.Add(1)
	})
	if !renewed {
		t.Fatalf("renewed = false, want true (single transient failure on a 300ms lease must recover in-window, not abandon)")
	}
	if n := lost.Load(); n != 0 {
		t.Fatalf("lease-loss signals = %d, want 0 (recovered turn must not abandon)", n)
	}
	if n := store.calls.Load(); n != 1 {
		t.Fatalf("ExtendLease calls = %d, want 1 (exactly the in-window retry)", n)
	}
	if at.Before(lastSuccess) {
		t.Fatalf("renewal instant %v precedes lastSuccess %v", at, lastSuccess)
	}
}
