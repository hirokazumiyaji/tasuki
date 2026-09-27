package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// errRound24Injected is the synthetic renewal failure for round-24
// scheduling tests.
var errRound24Injected = errors.New("injected round-24 renewal failure")

// round24FirstCallBackend counts ExtendLease calls and records the first
// call's latency from a reference instant, so renewal-scheduling tests can
// observe when the loop first reaches the store.
type round24FirstCallBackend struct {
	backend.Backend
	calls     atomic.Int32
	firstLat  atomic.Int64
	startUnix atomic.Int64
	fail      bool
}

func (b *round24FirstCallBackend) ExtendLease(context.Context, backend.Task, time.Duration) error {
	n := b.calls.Add(1)
	if n == 1 {
		b.firstLat.Store(time.Since(time.Unix(0, b.startUnix.Load())).Nanoseconds())
	}
	if b.fail {
		return errRound24Injected
	}
	return nil
}

// round24SlowSuccessBackend blocks the first ExtendLease for blockFor and
// then succeeds; every later call fails. It models a successful renewal
// response delayed in flight.
type round24SlowSuccessBackend struct {
	backend.Backend
	calls    atomic.Int32
	blockFor time.Duration
}

func (b *round24SlowSuccessBackend) ExtendLease(ctx context.Context, _ backend.Task, _ time.Duration) error {
	n := b.calls.Add(1)
	if n == 1 {
		timer := time.NewTimer(b.blockFor)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return nil
	}
	return errRound24Injected
}

// round24RetrySlowBackend fails the first two ExtendLease calls and blocks
// the third for blockFor before succeeding, so retryRenewal's success
// timestamp is observable against the delayed response.
type round24RetrySlowBackend struct {
	backend.Backend
	calls    atomic.Int32
	blockFor time.Duration
}

func (b *round24RetrySlowBackend) ExtendLease(ctx context.Context, _ backend.Task, _ time.Duration) error {
	n := b.calls.Add(1)
	if n < 3 {
		return errRound24Injected
	}
	timer := time.NewTimer(b.blockFor)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return nil
}

// TestWorker_Round24_FirstRenewalFromClaimTime is the regression test for
// round-24 P2a (schedule the first renewal from the claim timestamp). A
// ClaimTasks that consumed more than half the lease leaves the original
// lease nearly expired when the renewal loop starts; a plain half-lease
// ticker then waits a full half-lease more while a peer reclaims mid-turn.
//
// Layout: 1s lease, claimBase 900ms in the past (the slow claim), store
// down. The first attempt must fire almost immediately — well before the
// 500ms half-lease tick — and abandon at once since the deadline already
// passed. Without the fix the first ExtendLease lands at ~500ms, past the
// 1s store expiry, where a peer may already own the lease.
func TestWorker_Round24_FirstRenewalFromClaimTime(t *testing.T) {
	ctx := context.Background()
	const lease = time.Second
	store := &round24FirstCallBackend{fail: true}
	store.startUnix.Store(time.Now().UnixNano())
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		// The claim itself took ~900ms to return (slow store scan); the
		// loop starts now with only ~100ms of lease left.
		w.extendLeaseLoop(ctx, backend.Task{ID: 7}, done, func() { lost.Add(1) }, time.Now().Add(-900*time.Millisecond))
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
		t.Fatalf("ExtendLease calls = %d, want 1 (the immediate tick only; no post-deadline retries)", n)
	}
	// The half-lease tick is 500ms out; the claim-aware first attempt
	// fires at once. 300ms splits immediate from tick with margin for
	// goroutine scheduling jitter on either side.
	if lat := time.Duration(store.firstLat.Load()); lat >= 300*time.Millisecond {
		t.Fatalf("first renewal after %v, want < 300ms (a claim that consumed most of the lease must renew almost immediately, not wait for the half-lease tick)", lat)
	}
}

// TestWorker_Round24_SuccessStampsRequestStart is the regression test for
// round-24 P2b (base the renewed lease expiry on the request start). A
// successful ExtendLease response delayed in flight must not move
// lastSuccess to the response time: the store lease was already aging
// while the call was blocked, so the following retry window would stretch
// past the actual store expiry and keep the turn alive through a peer
// reclaim.
//
// Layout: 1s lease (margin 250ms, tick 500ms), first renewal blocked 600ms
// then succeeding, everything after failing. The success stamps the
// pre-call instant (~500ms), so the failed second tick abandons at
// ~1250ms. Without the fix the success stamps the response (~1100ms) and
// the loop retries until ~1850ms — past the 1s store expiry.
func TestWorker_Round24_SuccessStampsRequestStart(t *testing.T) {
	ctx := context.Background()
	const lease = time.Second
	store := &round24SlowSuccessBackend{blockFor: 600 * time.Millisecond}
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	start := time.Now()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		w.extendLeaseLoop(ctx, backend.Task{ID: 7}, done, func() { lost.Add(1) }, start)
	}()
	select {
	case <-loopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("renewal loop did not abandon after the delayed success")
	}
	elapsed := time.Since(start)
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1", n)
	}
	// Fixed loop abandons at ~1250ms; the response-stamped loop holds
	// until ~1850ms. 1550ms splits the two with margin for timer jitter.
	if elapsed >= 1550*time.Millisecond {
		t.Fatalf("abandoned after %v, want well before 1550ms (the delayed success must stamp the ~500ms request start, so the failed next tick abandons at ~1250ms instead of retrying past the 1s store expiry)", elapsed)
	}
}

// TestWorker_Round24_RetrySuccessStampsRequestStart pins the same
// conservatism on the retry-success path: retryRenewal must report the
// pre-call instant, not the delayed response, so the caller's window never
// stretches past the actual store lease.
func TestWorker_Round24_RetrySuccessStampsRequestStart(t *testing.T) {
	ctx := context.Background()
	const lease = 5 * time.Second
	store := &round24RetrySlowBackend{blockFor: 300 * time.Millisecond}
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	start := time.Now()
	at, renewed := w.retryRenewal(ctx, backend.Task{ID: 7}, done, lease, start, nil)
	end := time.Now()
	if !renewed {
		t.Fatal("renewed = false, want true (the third attempt succeeds after the delay)")
	}
	// Backoffs 100ms + 200ms + 400ms put the winning call's start at
	// ~700ms and its delayed response at ~1000ms. The reported instant
	// must sit near the request start, not the response: 850ms splits
	// the two with margin for timer/scheduling jitter.
	if d := at.Sub(start); d >= 850*time.Millisecond {
		t.Fatalf("renewal instant = %v after start (total call took %v), want the pre-call instant well before 850ms", d, end.Sub(start))
	}
	if n := store.calls.Load(); n != 3 {
		t.Fatalf("ExtendLease calls = %d, want 3 (two fast failures plus the delayed success)", n)
	}
}
