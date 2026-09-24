package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_Round17_UnrelatedRenewalDoesNotBlockExclusiveCommit is the
// regression test for round-17 P1 (scope renewal joins to the committing
// task): the pre-fix joinOrdinaryRenewalsForCommit waited on the
// worker-GLOBAL idle channel, so one unrelated slow/stuck admitted
// ordinary ExtendLease — or an old-generation renewal surviving a bounded
// shutdown, or a stream of new unrelated renewals — kept the global count
// nonzero while every row-preserving retry/nack held its activity slot
// with no result write and no bound from the commit context.
//
// The test holds one admitted ordinary renewal for an UNRELATED task ID
// across a row-preserving commit for the committing task. With the fix
// the commit joins only its own task's renewals and proceeds immediately;
// without it the commit parks on the global barrier until the unrelated
// slot drains.
func TestWorker_Round17_UnrelatedRenewalDoesNotBlockExclusiveCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round17-unrelated-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	// Hold one admitted ordinary renewal for an unrelated task ID, as if
	// another task's ticker were blocked inside ExtendLease.
	const unrelatedID int64 = 987654321
	tokOther := w.track(unrelatedID)
	defer w.untrack(unrelatedID, tokOther)
	if !w.renewTryEnter(unrelatedID, tokOther) {
		t.Fatal("renewTryEnter(unrelated) = false, want true (fresh lease must admit the ordinary renewal)")
	}
	defer w.renewExit(unrelatedID)

	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}

	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	var opDone atomic.Bool
	commitCh := make(chan error, 1)
	go func() {
		commitCh <- w.guardedDetachedCommit(task.ID, tok, commitCtx, commitCancel, true, func() error {
			opDone.Store(true)
			return mem.RetryActivity(commitCtx, task.ID, 5*time.Second)
		})
	}()
	select {
	case cerr := <-commitCh:
		if cerr != nil {
			t.Fatalf("exclusive commit = %v, want nil (unrelated renewal must not block it)", cerr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("exclusive commit blocked >3s on an unrelated task's renewal (commit must join only its own task)")
	}
	if !opDone.Load() {
		t.Fatal("exclusive commit op never ran (unrelated renewal must not block it)")
	}

	// The retry delay must stand: at t0+3s the task is still hidden.
	mem.SetNow(t0.Add(3 * time.Second))
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 0 {
		t.Fatalf("peer claimed %d tasks, want 0 (retry delay must stand)", len(probe))
	}
}

// TestWorker_Round17_CommitJoinBoundedByCommitContext covers the second
// half of the round-17 P1 fix: even a SAME-task stuck renewal must not
// hold the activity slot forever. The per-task join is bounded by the
// commit context (plus commitJoinCap), so a canceled commit context
// aborts the join without running the row-preserving write — the stuck
// renewal landing later only extends the lease instead of overwriting a
// result. Without the bound the commit parks until the renewal drains.
func TestWorker_Round17_CommitJoinBoundedByCommitContext(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round17-cancel-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	// Hold this task's own ordinary renewal so the join has something to
	// wait on.
	if !w.renewTryEnter(task.ID, tok) {
		t.Fatal("renewTryEnter = false, want true (fresh lease must admit the ordinary renewal)")
	}
	defer w.renewExit(task.ID)

	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}

	canceledCtx, canceledCancel := context.WithCancel(context.Background())
	canceledCancel()
	opRan := false
	done := make(chan error, 1)
	go func() {
		done <- w.guardedDetachedCommit(task.ID, tok, canceledCtx, canceledCancel, true, func() error {
			opRan = true
			return nil
		})
	}()
	select {
	case gerr := <-done:
		if !errors.Is(gerr, errLeaseLost) {
			t.Fatalf("guardedDetachedCommit = %v, want errLeaseLost (canceled commit context must bound the join)", gerr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("exclusive commit blocked >3s on a same-task renewal with a canceled commit context (join must be cancelable)")
	}
	if opRan {
		t.Fatal("guardedDetachedCommit ran the store op after the join gave up (must issue nothing)")
	}
	// The guard is dropped on the abort path so cover stops too.
	w.detMu.Lock()
	_, stillGuarded := w.detGuard[task.ID]
	w.detMu.Unlock()
	if stillGuarded {
		t.Fatal("detached guard still present after the join-timeout abort (must be dropped)")
	}
}
