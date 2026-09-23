package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// countExtendBackend counts ExtendLease and CompleteActivity calls while
// delegating to the wrapped backend, so tests can assert a renewal was
// attempted (success path) while its commit was skipped.
type countExtendBackend struct {
	backend.Backend
	extends   atomic.Int32
	completes atomic.Int32
}

func (b *countExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	b.extends.Add(1)
	return b.Backend.ExtendLease(ctx, taskID, d)
}

func (b *countExtendBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	b.completes.Add(1)
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

// TestWorker_Round9_ShutdownJoinsInFlightRenewal is a regression test for
// round-9 P1a (ordinary renewal vs shutdown release): grace expiry while
// extendLeaseLoop is inside ExtendLease leaves a renewal that completes
// despite the execution-context cancel. Shutdown must join it before
// releaseInFlight — releasing first lets the renewal land after the
// ReleaseLease, re-hiding the released task (or extending a peer's fresh
// lease through the ID-only ExtendLease).
//
// The test holds one ordinary renewal "in flight" (a join slot claimed
// the way the ticker path claims its slot) across Shutdown's grace
// expiry with a cancel-ignoring activity still running: with the fix the
// release waits for the slot; without it the release fires at grace
// expiry while the renewal is still outstanding.
func TestWorker_Round9_ShutdownJoinsInFlightRenewal(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &joinOrderBackend{Backend: mem}

	allowReturn := make(chan struct{})
	entered := make(chan struct{})
	var enterOnce atomic.Bool
	w := NewWorker(store, WorkerOptions{
		PollInterval:           5 * time.Millisecond,
		LeaseDuration:          30 * time.Second, // no ticker renewal during the test
		WorkerID:               "w1",
		ShutdownReleaseTimeout: 10 * time.Second,
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		if enterOnce.CompareAndSwap(false, true) {
			close(entered)
		}
		<-allowReturn // ignores cancellation: still running past the grace
		return "ok", nil
	}, WithName("hooked"))
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "hooked", struct{}{})
	}, WithName("WF"))
	c := NewClient(store)
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID("round9-join-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("activity was not claimed")
	}

	// One ordinary renewal outstanding, as if the ticker path were
	// blocked inside ExtendLease when the grace expires.
	w.renewWg.Add(1)
	shCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() { _ = w.Shutdown(shCtx); close(shutdownDone) }()

	// Grace (400ms) expires here; the fix holds the release for the
	// outstanding renewal slot, the old code releases immediately.
	time.Sleep(800 * time.Millisecond)
	if store.hasEvent("release-enter") {
		w.renewWg.Done()
		close(allowReturn)
		<-shutdownDone
		t.Fatal("release fired while an ordinary renewal was still in flight (Shutdown must join renewals first)")
	}

	w.renewWg.Done()
	close(allowReturn)
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	if n := countEvents(store.snapshot(), "extend-enter"); n != 0 {
		t.Fatalf("ExtendLease calls=%d, want 0 (30s lease: no ticker renewal during the test)", n)
	}
	if !store.hasEvent("release-enter") {
		t.Fatalf("no release recorded, events=%v", store.snapshot())
	}
	if got := countEvents(store.snapshot(), "release-enter"); got != 1 {
		t.Fatalf("release-enter count=%d, want 1 (only the still-running activity lease)", got)
	}
}

func countEvents(evs []string, name string) int {
	n := 0
	for _, e := range evs {
		if e == name {
			n++
		}
	}
	return n
}

// TestWorker_Round9_OrdinaryRenewalStopsAtShutdownFlag covers the other
// half of the join: once Shutdown sets the stop flag, the ticker path
// must not issue new ordinary renewals — even with a live-looking entry
// — so none can land after the release.
func TestWorker_Round9_OrdinaryRenewalStopsAtShutdownFlag(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &extendRecorder{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond, // 100ms tick
		WorkerID:      "w1",
	})
	tok := w.track(71)
	defer w.untrack(71, tok)

	w.renewStop.Store(true) // Shutdown grace expiry, before the loop runs
	var committing atomic.Bool
	done := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.extendLeaseLoop(context.Background(), 71, tok, done, &committing)
	}()
	select {
	case <-renewDone:
	case <-time.After(2 * time.Second):
		close(done)
		t.Fatal("renewal loop did not exit after the shutdown stop flag (it would renew past the release)")
	}
	close(done)
	if n := store.extendCount(); n != 0 {
		t.Fatalf("ExtendLease calls=%d, want 0 (no new ordinary renewal after the stop flag)", n)
	}
}

// TestWorker_Round9_OrdinaryRenewalStopsAfterLocalExpiry covers the
// ordinary-path half of round-9 P1b: renewals keep failing, the local
// lease expires (a peer may have reclaimed the task), and a later
// success must not extend the peer's lease — the loop stops instead of
// renewing on an unfresh lease. Without the fix the loop renews
// forever on the stale entry.
func TestWorker_Round9_OrdinaryRenewalStopsAfterLocalExpiry(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &failExtendBackend{Backend: mem} // every renewal fails
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond, // 100ms tick, local expiry ~200ms
		WorkerID:      "w1",
	})
	tok := w.track(72)
	defer w.untrack(72, tok)

	var committing atomic.Bool
	done := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.extendLeaseLoop(context.Background(), 72, tok, done, &committing)
	}()
	select {
	case <-renewDone:
	case <-time.After(3 * time.Second):
		close(done)
		t.Fatal("renewal loop did not stop after local lease expiry (it would extend a peer's lease on recovery)")
	}
	close(done)
	// Only pre-expiry ticks may have issued: ~100ms tick vs ~200ms
	// lease. Without the fix the loop never stops (~6 calls in 600ms+
	// and renewDone stays open).
	if n := store.extends.Load(); n > 2 {
		t.Fatalf("ExtendLease calls=%d, want <=2 (only pre-expiry ticks; loop must stop once unfresh)", n)
	}
}

// TestWorker_Round9_DetachedRenewalSuccessAfterExpiryAbortsCommit is the
// core regression test for round-9 P1b: the lease expired (plus an
// earlier renewal gap), and the pre-commit detached renewal SUCCEEDS
// via the ID-only ExtendLease — extending whatever lease is there now,
// possibly a peer's. Success must not count as proof of ownership: the
// worker-side continuity guard aborts the commit instead of letting the
// stale completion modify the peer's task.
//
// Without the fix the synchronous renewal succeeds and the commit
// proceeds (no error); with the fix it fails with errLeaseLost after
// attempting the renewal (extends>=1 proves this is the success path,
// not the round-8 failure path).
func TestWorker_Round9_DetachedRenewalSuccessAfterExpiryAbortsCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round9-lost-1", "hooked")
	store := &countExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	// Let the worker-side continuity deadline pass with no successful
	// renewal in between: the backend lease is now suspect (a peer may
	// own it), even though the next ExtendLease will succeed.
	time.Sleep(300 * time.Millisecond)

	renewDone := make(chan struct{})
	defer close(renewDone)
	detachedEntered := make(chan struct{})
	defer close(detachedEntered)
	_, err := w.ensureCommitRenewal(ctx, task.ID, tok, renewDone, detachedEntered)
	if err == nil {
		t.Fatal("ensureCommitRenewal = nil, want errLeaseLost (post-expiry renewal success is not ownership)")
	}
	if !errors.Is(err, errLeaseLost) {
		t.Fatalf("ensureCommitRenewal = %v, want errLeaseLost", err)
	}
	if n := store.extends.Load(); n == 0 {
		t.Fatal("ExtendLease calls=0, want >=1 (the renewal must be attempted; its success is what gets rejected)")
	}
	if n := store.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls=%d, want 0 (commit must be skipped on success-without-ownership)", n)
	}
}

// TestWorker_Round9_DetachedCoverStopsOnLeaseLoss covers the cover
// loops: once the guard reports the lease moved on, renewUntilDone
// stops instead of extending the peer's lease every half-lease. Without
// the fix it renews until done closes.
func TestWorker_Round9_DetachedCoverStopsOnLeaseLoss(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round9-lost-2", "hooked")
	store := &countExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	time.Sleep(300 * time.Millisecond) // pass the continuity deadline

	done := make(chan struct{})
	defer close(done)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		w.renewUntilDone(context.Background(), task.ID, tok, done, ticker, &committing)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("renewUntilDone did not stop on lease loss (it would keep extending a peer's lease)")
	}
	// Only the immediate entry renewal may have issued; no periodic
	// cover follows a lost lease. Without the fix every 50ms tick
	// renews (~10 in 500ms).
	if n := store.extends.Load(); n != 1 {
		t.Fatalf("ExtendLease calls=%d, want 1 (immediate attempt only; cover must stop on loss)", n)
	}
}
