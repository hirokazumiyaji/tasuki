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
)

// TestWorker_Round8_HandoffWaitsForExitedLoop is a regression test for
// round-8 P1a (renewal-loop handoff race): a cancel landing after the
// post-invoke live-context check but before beginDetachedCommit lets the
// renewal loop read committing==false, decide to return, and pause before
// closing renewDone. beginDetachedCommit then sets the flag while renewDone
// is still open; a nonblocking check mistakes the open channel for a live
// loop, starts no replacement, and the slow commit outlives the lease onto
// a peer's task.
//
// The test drives the handoff directly: execution ctx is canceled,
// renewDone stays open for 200ms (loop paused before close), detached ack
// never closes (loop committed to exiting). With the fix ensureCommitRenewal
// joins the old loop's fate (waits for renewDone close), runs a synchronous
// detached renewal, and starts a replacement. Without the fix it returns
// immediately with a no-op and no renewal (ExtendLease count 0).
func TestWorker_Round8_HandoffWaitsForExitedLoop(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &extendRecorder{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond,
		WorkerID:      "w1",
	})
	tok := w.track(42)
	defer w.untrack(42, tok)

	renewDone := make(chan struct{})
	detachedEntered := make(chan struct{})
	// Simulate the paused-before-close loop: close renewDone after 200ms.
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(renewDone)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // execution canceled: liveness ambiguous, must join fate

	start := time.Now()
	stop, err := w.ensureCommitRenewal(ctx, 42, tok, renewDone, detachedEntered)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ensureCommitRenewal = %v, want nil (sync renewal succeeds)", err)
	}
	if stop == nil {
		t.Fatal("ensureCommitRenewal stop is nil")
	}
	defer stop()
	// Must have waited for the loop's fate, not returned on an open channel.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("ensureCommitRenewal returned in %v, want >=150ms (must join renewDone close, not assume alive)", elapsed)
	}
	// Must have run the synchronous pre-commit renewal.
	if n := store.extendCount(); n == 0 {
		t.Fatalf("ExtendLease calls=0, want >=1 (synchronous pre-commit renewal before the store commit)")
	}
}

// TestWorker_Round8_HandoffReusesDetachedLoop covers the other fate: the
// loop observes the flag and enters detached renewal (ack closes) while
// renewDone stays open. The commit must wait for the ack, run the
// synchronous renewal, and reuse the inherited loop (no replacement needed,
// but renewal must still be verified).
func TestWorker_Round8_HandoffReusesDetachedLoop(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &extendRecorder{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond,
		WorkerID:      "w1",
	})
	tok := w.track(43)
	defer w.untrack(43, tok)

	renewDone := make(chan struct{})
	defer close(renewDone)
	detachedEntered := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(detachedEntered)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	stop, err := w.ensureCommitRenewal(ctx, 43, tok, renewDone, detachedEntered)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ensureCommitRenewal = %v, want nil", err)
	}
	if stop == nil {
		t.Fatal("stop is nil")
	}
	defer stop()
	if elapsed < 100*time.Millisecond {
		t.Fatalf("returned in %v, want >=100ms (must wait for detached ack)", elapsed)
	}
	if n := store.extendCount(); n == 0 {
		t.Fatalf("ExtendLease calls=0, want >=1 (pre-commit renewal even when reusing the loop)")
	}
}

// failExtendBackend fails every detached ExtendLease with a transient error
// while counting CompleteActivity calls, so the test can assert the commit
// was skipped.
type failExtendBackend struct {
	backend.Backend
	extends   atomic.Int32
	completes atomic.Int32
}

func (b *failExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.extends.Add(1)
	return errors.New("transient store timeout")
}

func (b *failExtendBackend) CompleteActivity(ctx context.Context, task backend.Task, ev journal.Event) error {
	b.completes.Add(1)
	return b.Backend.CompleteActivity(ctx, task, ev)
}

// TestWorker_Round8_DetachedRenewalFailureAbortsCommit is a regression test
// for round-8 P1b: a detached ExtendLease transient error/timeout must abort
// the result commit (lease-lost, no store touch) instead of only logging and
// letting the ID-only Complete modify a peer's reclaimed task.
//
// Without the fix the commit proceeds (completes==1); with the fix the
// pre-commit synchronous renewal fails and the commit is skipped
// (completes==0, lease-lost error).
func TestWorker_Round8_DetachedRenewalFailureAbortsCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	// Set up the claimable task with a healthy worker first: the failing
	// backend would otherwise break the setup's own renewal.
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round8-abort-1", "hooked")
	store := &failExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "ok", nil
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	err := w.handleActivity(ctx, task, tok)
	if err == nil {
		t.Fatal("handleActivity = nil, want lease-lost error (detached renewal failed)")
	}
	if !errors.Is(err, errLeaseLost) {
		t.Fatalf("handleActivity = %v, want errLeaseLost", err)
	}
	if n := store.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls=%d, want 0 (commit must be skipped on renewal failure)", n)
	}
	if n := store.extends.Load(); n == 0 {
		t.Fatalf("ExtendLease calls=0, want >=1 (pre-commit renewal must run)")
	}
	// The task must not have been deleted by a stale commit: the instance
	// is still running and the task row still exists for natural expiry
	// reclaim.
	c := NewClient(store)
	h, herr := Start(ctx, c, "WF", struct{}{}, WithID("round8-abort-1-probe"))
	_ = h
	_ = herr
	// Probe via a fresh claim with a different worker after moving the store
	// clock past the lease: the aborted task must still be reclaimable
	// (it was never completed).
	mem.SetNow(t0.Add(10 * time.Second))
	peer, perr := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if perr != nil {
		t.Fatal(perr)
	}
	if len(peer) == 0 {
		t.Fatal("aborted task was never reclaimable (expected natural expiry reclaim)")
	}
}
