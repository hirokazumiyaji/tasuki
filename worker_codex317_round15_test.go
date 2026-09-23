package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_Round15_RenewalBarrierGenerationScoped is the regression test
// for round-15 P1 (renewal stop barrier generation-scoped): an ordinary
// ticker paused between ownsFresh and renewTryEnter across grace expiry +
// restart must not observe the new generation's reset stop flag and issue
// an ID-only ExtendLease against the reclaimed task.
//
// The test drives the exact interleaving deterministically: track in
// generation 1, verify freshness (ticker's pre-check), join with zero
// in-flight (grace expiry sees nothing), release, full Shutdown + Start
// (new generation re-arms the barrier and reclaims the same task ID),
// then resume the old ticker. With the fix the stale admission is rejected
// by its token epoch; without it (stop flag only) the reset flag admits it
// and the ExtendLease would extend the new owner's lease.
func TestWorker_Round15_RenewalBarrierGenerationScoped(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		PollInterval:           50 * time.Millisecond,
		LeaseDuration:          30 * time.Second,
		WorkerID:               "w1",
		ShutdownReleaseTimeout: 5 * time.Second,
	})
	if err := w.StartWithError(ctx); err != nil {
		t.Fatal(err)
	}
	const taskID int64 = 424201
	tokOld := w.track(taskID)
	// Ticker's freshness pre-check passes; the goroutine pauses here.
	if !w.ownsFresh(taskID, tokOld) {
		_ = w.Shutdown(context.Background())
		t.Fatal("ownsFresh = false, want true (fresh 30s lease; test did not set up the pause window)")
	}
	// Grace expiry before the ticker resumes: no renewal registered, so
	// the join sets the stop flag and returns immediately.
	w.shutdownRenewalJoin(context.Background())
	// Shutdown's lease release takes the task.
	if !w.claimReleaseOwnership(taskID, tokOld) {
		_ = w.Shutdown(context.Background())
		t.Fatal("claimReleaseOwnership = false, want true (fresh lease must be releasable)")
	}
	// Restart before the old ticker resumes: the barrier re-arms for the
	// new generation only, and the same task ID is reclaimed there.
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if err := w.StartWithError(ctx); err != nil {
		t.Fatalf("restart StartWithError = %v, want nil", err)
	}
	tokNew := w.track(taskID)
	defer w.untrack(taskID, tokNew)
	defer func() { _ = w.Shutdown(context.Background()) }()

	// The old ticker resumes: even though the stop flag is clear for the
	// new generation, its stale epoch must be rejected.
	if w.renewTryEnter(tokOld) {
		w.renewExit()
		t.Fatal("renewTryEnter(stale) = true, want false (old generation must not observe the new generation's re-arm)")
	}
	// The new generation is admitted.
	if !w.renewTryEnter(tokNew) {
		t.Fatal("renewTryEnter(current) = false, want true (re-arm must admit the new generation)")
	}
	w.renewExit()
}

// TestWorker_Round15_DeadlineExpiryCancelsCover is the regression test for
// round-15 P2 (cancel cover renewal when the commit deadline expires): a
// commit reaching guardedDetachedCommit after the continuity deadline with
// a cover ExtendLease already blocked must drop the guard AND cancel the
// commit's cover context. Without the cancel the blocked renewal stays
// live and lands by ID on the peer-reclaimed task, and the deferred
// joinCommitStop holds its slot until the lease-bounded renewal timeout
// even though the result is rejected here.
//
// The test holds one cover renewal blocked in a context-aware backend,
// expires the guard deadline, then gates a commit: with the fix the gate
// returns errLeaseLost without running the op, the cover context is
// canceled, and the blocked renewal aborts quickly (well under the 10s
// lease timeout) without landing a backend write. Without the fix the
// cover stays live and the renewal never aborts until released.
func TestWorker_Round15_DeadlineExpiryCancelsCover(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &stallExtendBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round15-deadline-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}

	// One cover renewal enters ExtendLease and blocks there.
	renewCh := make(chan error, 1)
	go func() { renewCh <- w.renewOnceDetached(ctx, task.ID, tok) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		close(store.release)
		t.Fatal("cover renewal never entered ExtendLease")
	}

	// Expire the continuity deadline while the renewal is still blocked,
	// then capture the cover context to assert cancellation below.
	w.detMu.Lock()
	g, ok := w.detGuard[task.ID]
	if !ok {
		w.detMu.Unlock()
		close(store.release)
		t.Fatal("detached guard missing before the deadline-expiry gate")
	}
	coverCtx := g.coverCtx
	g.deadline = time.Now().Add(-time.Second)
	w.detGuard[task.ID] = g
	w.detMu.Unlock()

	// The commit reaches the gate after the deadline: it must be rejected
	// without running the store op.
	opRan := false
	_, commitCancel := context.WithCancel(context.Background())
	defer commitCancel()
	gerr := w.guardedDetachedCommit(task.ID, tok, commitCancel, true, func() error {
		opRan = true
		return nil
	})
	if !errors.Is(gerr, errLeaseLost) {
		close(store.release)
		t.Fatalf("guardedDetachedCommit = %v, want errLeaseLost (deadline expired before the store op)", gerr)
	}
	if opRan {
		close(store.release)
		t.Fatal("guardedDetachedCommit ran the store op past the continuity deadline (must issue nothing)")
	}
	// The guard is dropped.
	w.detMu.Lock()
	_, stillGuarded := w.detGuard[task.ID]
	w.detMu.Unlock()
	if stillGuarded {
		close(store.release)
		t.Fatal("detached guard still present after the deadline-expiry gate (must be dropped)")
	}
	// The cover is canceled: a context-aware backend drops the blocked
	// write instead of landing it after the rejection.
	if coverCtx == nil || coverCtx.Err() == nil {
		close(store.release)
		t.Fatal("cover context not canceled on the deadline-expiry path (blocked renewal left live)")
	}
	// The blocked renewal aborts quickly — well under its 10s
	// lease-bounded timeout — so only the gate's cancel (not the timeout
	// backstop) can pass this.
	select {
	case rerr := <-renewCh:
		if !errors.Is(rerr, errLeaseLost) {
			close(store.release)
			t.Fatalf("aborted cover renewal = %v, want errLeaseLost", rerr)
		}
	case <-time.After(3 * time.Second):
		close(store.release)
		t.Fatal("blocked cover renewal was not aborted by the deadline-expiry gate (join would hold its slot until the lease timeout)")
	}
	if n := store.landedCount(); n != 0 {
		close(store.release)
		t.Fatalf("landed ExtendLease writes=%d, want 0 (in-flight renewal must be dropped, not landed after the rejection)", n)
	}
	close(store.release)
	// Releasing the gate now must change nothing: the write was already
	// dropped.
	time.Sleep(100 * time.Millisecond)
	if n := store.landedCount(); n != 0 {
		t.Fatalf("landed ExtendLease writes=%d, want 0 (released gate must not land a dropped write)", n)
	}
}
