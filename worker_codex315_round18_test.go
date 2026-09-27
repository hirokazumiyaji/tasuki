package tasuki

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// countCommitBackend counts advancement store calls while delegating
// everything else. It embeds the backend interface (not a batching
// implementation), so the worker takes the per-commit flush path and a
// test fails if any commit reaches the store.
type countCommitBackend struct {
	backend.Backend
	singles atomic.Int32
}

func (b *countCommitBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.singles.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *countCommitBackend) storeCalls() int32 {
	return b.singles.Load()
}

// gateCommitBackend blocks the first CommitAdvancement on a
// test-controlled gate (then delegates) while counting later calls, so
// a test can land cancellation while one commit is in flight and prove
// the remainder of the flush never reaches the store.
type gateCommitBackend struct {
	backend.Backend
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (b *gateCommitBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	if b.calls.Add(1) == 1 {
		if b.once.CompareAndSwap(false, true) {
			close(b.entered)
		}
		<-b.release
	}
	return b.Backend.CommitAdvancement(ctx, adv)
}

// startSimpleWorkflow registers a immediately-completing workflow under
// name and starts one instance, returning its ID.
func startSimpleWorkflow(t *testing.T, ctx context.Context, w *Worker, c *Client, name, instanceID string) string {
	t.Helper()
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName(name))
	h, err := Start(ctx, c, name, struct{}{}, WithID(instanceID))
	if err != nil {
		t.Fatal(err)
	}
	return h.ID()
}

// claimWorkflowTask claims one workflow task for workerID, failing the
// test unless exactly one is returned.
func claimWorkflowTask(t *testing.T, ctx context.Context, mem *memory.Backend, workerID string) backend.Task {
	t.Helper()
	tasks, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: 30 * time.Second, WorkerID: workerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("claimed %d workflow tasks, want 1", len(tasks))
	}
	return tasks[0]
}

// TestWorker_Round18_CanceledFlushIssuesNoStoreCalls is the regression
// test for round-18 P1 (re-check cancellation before each flush store
// call): tickWorkflows checks ctx.Err() before the flush, but Shutdown
// can cancel in the check-to-flush window, and a context-insensitive
// backend (memory) would then persist the advancement instead of
// abandoning it. With the fix the flush issues no store call under an
// already-canceled context and returns the pending as failed so the
// caller releases the lease. Without the fix the commit reaches the
// store (memory ignores the cancellation) and the advancement persists.
func TestWorker_Round18_CanceledFlushIssuesNoStoreCalls(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &countCommitBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:       30 * time.Second,
		WorkerID:            "w1",
		ClaimLimit:          10,
		WorkflowConcurrency: 4,
	})
	c := NewClient(store)
	id := startSimpleWorkflow(t, ctx, w, c, "round18-cancel-flush", "round18-cancel-flush-1")

	// Build a genuine pending through the production turn path with a
	// live context.
	task := claimWorkflowTask(t, ctx, mem, "w1")
	w.track(task)
	p, herr := w.handleWorkflow(ctx, task, nil)
	if herr != nil {
		t.Fatalf("handleWorkflow = %v, want nil (completing turn must build a pending)", herr)
	}
	if p == nil {
		t.Fatal("handleWorkflow returned no pending for a completing turn")
	}
	p.task = task
	if n := store.storeCalls(); n != 0 {
		t.Fatalf("store calls during the turn = %d, want 0 (success path must not touch the store before the flush)", n)
	}

	// Shutdown cancels between the tick's canceled-check and the flush:
	// drive the flush itself with the canceled context.
	tickCtx, cancel := context.WithCancel(context.Background())
	cancel()
	failed := w.flushWorkflowCommits(tickCtx, []pendingWorkflowCommit{*p})
	if n := store.storeCalls(); n != 0 {
		t.Fatalf("flush store calls under a canceled tick = %d, want 0 (cancel before the store call must skip it)", n)
	}
	if len(failed) != 1 {
		t.Fatalf("failed = %d pendings, want 1 (canceled commit must return for disposal, not vanish)", len(failed))
	}

	// Nothing persisted: the instance is still running with no terminal
	// event, and the task is still tracked for the caller's disposal.
	head, err := mem.LoadWorkflowHead(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if head.Instance.Status != "running" {
		t.Fatalf("status = %s, want running (canceled flush must not persist the advancement)", head.Instance.Status)
	}
	w.mu.Lock()
	_, tracked := w.inFlight[task.ID]
	w.mu.Unlock()
	if !tracked {
		t.Fatal("pending task left the in-flight set on a skipped flush (caller must still own it for disposal)")
	}
}

// TestWorker_Round18_CancelBetweenCommitsSkipsRemainder covers the
// mid-flush half of round-18 P1: with two pendings, a cancel landing
// while the first commit is blocked in the backend must not let the
// second commit reach the store. The first commit is already inside
// its backend call when cancellation lands, so on a
// context-insensitive backend it still persists (documented residual);
// its fenced generation keeps a peer from double-executing, and the
// second pending is skipped and released for a prompt peer retry.
// Without the per-call gate both commits persist.
func TestWorker_Round18_CancelBetweenCommitsSkipsRemainder(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &gateCommitBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       30 * time.Second,
		WorkerID:            "w1",
		ClaimLimit:          10,
		WorkflowConcurrency: 4,
	})
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(store)
	idA := "round18-midflush-A"
	idB := "round18-midflush-B"
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID(idA)); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID(idB)); err != nil {
		t.Fatal(err)
	}

	tickCtx, cancel := context.WithCancel(context.Background())
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()
	// The first commit is now blocked inside its backend call with the
	// tick context still live (its pre-call gate passed legitimately).
	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("first workflow commit never reached the store")
	}
	// Shutdown cancels mid-flush; then let the blocked call through. On
	// memory (context-insensitive) it still persists — the documented
	// residual — while the second commit must never start.
	cancel()
	close(store.release)
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return (flush must not hang on cancellation)")
	}

	if n := store.calls.Load(); n != 1 {
		t.Fatalf("CommitAdvancement calls = %d, want 1 (cancel between commits must skip the remainder)", n)
	}
	// Exactly one instance advanced (the in-flight residual); the other
	// is still running and its task was released for a peer retry.
	status := func(id string) string {
		head, err := mem.LoadWorkflowHead(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return head.Instance.Status
	}
	stA, stB := status(idA), status(idB)
	if (stA == "completed") == (stB == "completed") {
		t.Fatalf("statuses = %s/%s, want exactly one completed (residual) and one running (skipped)", stA, stB)
	}
	running := idA
	if stA == "completed" {
		running = idB
	}
	peers, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: 30 * time.Second, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].InstanceID != running {
		got := make([]string, 0, len(peers))
		for _, p := range peers {
			got = append(got, p.InstanceID)
		}
		t.Fatalf("peer claimed %v, want exactly the released task of %s", got, running)
	}
	w.mu.Lock()
	left := len(w.inFlight)
	w.mu.Unlock()
	if left != 0 {
		t.Fatalf("inFlight = %d after the tick, want 0 (every pending must be committed or disposed)", left)
	}
}
