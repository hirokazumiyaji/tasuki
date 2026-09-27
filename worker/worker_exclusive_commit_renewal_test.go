package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_OrdinaryRenewalJoinedBeforeExclusiveCommit is the
// regression test for round-16 P1 (join ordinary renewals before
// row-preserving writes): an ordinary ticker renewal admitted via
// renewTryEnter just before beginDetachedCommit flips committing uses the
// execution context — not the commit's cover context — so the post-commit
// cover cancel cannot stop it. It can stay blocked while the sync
// pre-commit renewal succeeds and RetryActivity completes, then land after
// the result write and overwrite the retry delay. The deferred
// joinCommitStop waits only after the write, which is too late. The prior
// round-11 P1b test holds a cover renewal (renewOnceDetached) blocked
// post-transfer; this test holds an already-admitted ORDINARY renewal
// blocked across the transfer instead.
//
// With the fix, guardedDetachedCommit joins admitted ordinary renewals
// before a row-preserving op: the op does not run until the renewal
// settles, so the renewal lands first and the retry delay stands. Without
// the fix the op runs immediately while the renewal is still blocked and
// the released renewal overwrites visible_at afterwards (the peer probe
// reclaims the task early, and the op-done gate below fires before the
// release).
func TestWorker_OrdinaryRenewalJoinedBeforeExclusiveCommit(t *testing.T) {
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
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round16-ordinary-1", "hooked")

	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	// Admit the ordinary renewal BEFORE the commit transfer, exactly as a
	// ticker tick racing the handler would: registration succeeds, then
	// the ExtendLease call blocks in the backend.
	if !w.renewTryEnter(task.ID, tok) {
		t.Fatal("renewTryEnter = false, want true (fresh lease must admit the ordinary renewal)")
	}
	ordCh := make(chan error, 1)
	go func() {
		renewStart := time.Now()
		rerr := store.ExtendLease(ctx, task, 10*time.Second)
		w.renewExit(task.ID)
		if rerr == nil {
			w.refreshLeaseAt(task.ID, tok, renewStart)
		}
		ordCh <- rerr
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		w.renewExit(task.ID)
		t.Fatal("ordinary renewal never entered ExtendLease")
	}

	// Transfer to a detached commit while the ordinary renewal is still
	// blocked: committing flips, the guard seeds, and no new ordinary
	// renewal for this task may start — but the admitted one is live.
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing, context.Background()) {
		close(store.release)
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}

	// A row-preserving result commit runs while the ordinary renewal is
	// still blocked. With the fix it waits for the renewal to settle
	// before running the op; without it the op runs immediately.
	commitCtx, commitCancel := context.WithCancel(context.Background())
	defer commitCancel()
	var opDone atomic.Bool
	commitCh := make(chan error, 1)
	go func() {
		commitCh <- w.guardedDetachedCommit(task.ID, tok, commitCtx, commitCancel, true, func() error {
			opDone.Store(true)
			return mem.RetryActivity(commitCtx, task, 5*time.Second)
		})
	}()

	// Let the commit reach the gate/join. Without the fix the op fires
	// here while the renewal is still blocked; with the fix it stays
	// parked in the join until the release below.
	time.Sleep(200 * time.Millisecond)
	if opDone.Load() {
		close(store.release)
		select {
		case cerr := <-commitCh:
			_ = cerr
		case <-time.After(5 * time.Second):
		}
		select {
		case <-ordCh:
		case <-time.After(5 * time.Second):
		}
		t.Fatal("exclusive commit ran while an admitted ordinary renewal was still blocked (must join renewals before the row-preserving write)")
	}

	// Release the blocked renewal: it lands FIRST, then the joined commit
	// runs and overwrites it with the retry delay.
	close(store.release)
	select {
	case rerr := <-ordCh:
		if rerr != nil {
			t.Fatalf("ordinary renewal = %v, want nil (released gate must let the admitted write land before the result)", rerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary renewal did not return after the gate release")
	}
	select {
	case cerr := <-commitCh:
		if cerr != nil {
			t.Fatalf("exclusive commit = %v, want nil", cerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive commit did not run after the ordinary renewal settled")
	}
	if !opDone.Load() {
		t.Fatal("exclusive commit op never ran after the join")
	}

	// The retry delay must stand: at t0+3s the task is still hidden. An
	// ordinary renewal landing AFTER the RetryActivity would have
	// overwritten visible_at with the 10s lease and the probe would
	// reclaim it.
	mem.SetNow(t0.Add(3 * time.Second))
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 0 {
		t.Fatalf("peer claimed %d tasks, want 0 (retry delay must survive the overlapped ordinary renewal)", len(probe))
	}
}
