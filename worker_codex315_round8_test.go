package tasuki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorkflow_StaleCommitFencedAfterReclaim is a regression test for the
// round-8 P1 (commit fencing): a slow CommitAdvancement starting after the
// in-memory preflight loses its lease to a Shutdown release + peer reclaim.
// The backend must condition the transactional commit on the claimed
// generation (worker_id + attempt); the stale commit reports ErrConflict
// without deleting the peer's active task, and the worker treats it as lost
// (no commit retry). Without the fence the ID/sequence-only predicate
// deletes the peer task and persists the old advancement.
//
// Interleaving (memory backend): claim as w1, drive to a pending
// advancement (stamped w1), release + peer reclaim as w2, then commit the
// stale advancement directly. The fence rejects it; the peer lease stays
// intact and the instance stays running.
func TestWorkflow_StaleCommitFencedAfterReclaim(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(mem)
	h, err := Start(ctx, c, "WF", struct{}{}, WithID("fenced-commit-1"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %#v", err, claimed)
	}
	task := claimed[0]
	w.track(task)
	p, herr := w.handleWorkflow(ctx, task, func() {})
	if herr != nil {
		t.Fatalf("handleWorkflow = %v, want pending", herr)
	}
	if p == nil {
		t.Fatal("no pending advancement")
	}
	p.task = task
	// Advancement already stamped with w1 generation by handleWorkflow;
	// belt-and-braces for direct backend commit below.
	staleAdv := w.advForCommit(task, p.adv)
	if staleAdv.WorkerID == "" {
		t.Fatal("stale advancement carries no claim generation (worker must stamp)")
	}

	// Shutdown-timeout release + peer reclaim with a new generation.
	w.releaseInFlight(context.Background())
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer claimed %d, want 1", len(peer))
	}
	if peer[0].WorkerID == staleAdv.WorkerID && peer[0].Attempt == staleAdv.Attempt {
		t.Fatal("peer reclaim did not move the generation")
	}

	// The stale commit must fail without touching the peer's task.
	err = mem.CommitAdvancement(ctx, staleAdv)
	if !errors.Is(err, backend.ErrConflict) && !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale CommitAdvancement = %v, want ErrConflict/ErrNotFound (fenced)", err)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusRunning {
		t.Fatalf("status=%s, want running (stale commit must not persist)", info.Status)
	}
	third, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third claimed %d, want 0 (peer lease intact)", len(third))
	}

	// The worker treats the fenced failure as lost: commitWorkflow returns
	// the conflict, drops sticky, and never retries the commit (single
	// backend call, failed entry for the caller to untrack).
	w2 := NewWorker(mem, WorkerOptions{LeaseDuration: time.Minute, WorkerID: "w1"})
	cerr := w2.commitWorkflow(ctx, task, p.baseJournal, p.adv)
	if !errors.Is(cerr, backend.ErrConflict) && !errors.Is(cerr, backend.ErrNotFound) {
		t.Fatalf("commitWorkflow = %v, want conflict (lost, no retry)", cerr)
	}
	if info, _ := c.Get(ctx, h.ID()); info.Status != StatusRunning {
		t.Fatalf("status=%s after worker commit, want running", info.Status)
	}
}

// TestWorkflow_FreshCommitStillSucceeds guards the fence against
// over-matching: a commit carrying the current generation must succeed.
func TestWorkflow_FreshCommitStillSucceeds(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(mem)
	h, err := Start(ctx, c, "WF", struct{}{}, WithID("fenced-commit-2"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}
	task := claimed[0]
	w.track(task)
	p, herr := w.handleWorkflow(ctx, task, func() {})
	if herr != nil {
		t.Fatalf("handleWorkflow = %v", herr)
	}
	if p == nil {
		t.Fatal("no pending")
	}
	p.task = task
	if err := w.commitWorkflow(ctx, task, p.baseJournal, p.adv); err != nil {
		t.Fatalf("fresh commitWorkflow = %v, want nil", err)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusCompleted {
		t.Fatalf("status=%s, want completed", info.Status)
	}
}
