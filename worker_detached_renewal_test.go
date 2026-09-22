package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// gatedCommitBackend pins CompleteActivity on a test-controlled gate so the
// test can cancel the Start parent while a detached result commit is in
// flight. The commit uses a detached context, so parent cancellation does
// not abort the blocked call.
type gatedCommitBackend struct {
	backend.Backend
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *gatedCommitBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

// TestWorker_ParentCancelKeepsRenewalAliveThroughCommit covers the
// detached-commit / lease-renewal race: the Start parent is canceled (or the
// shutdown grace expires) while a detached result commit is still running.
// Lease renewal must stay alive until the commit finishes — otherwise a
// commit longer than the remaining lease races a peer reclaim and the
// task-ID-only commit deletes or reschedules the peer's task. Exactly one
// commit must land and a peer must never claim the task mid-commit.
func TestWorker_ParentCancelKeepsRenewalAliveThroughCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &gatedCommitBackend{Backend: mem, entered: make(chan struct{}), release: make(chan struct{})}

	var calls atomic.Int32
	newActivity := func() func(context.Context, struct{}) (string, error) {
		return func(context.Context, struct{}) (string, error) {
			calls.Add(1)
			return "ok", nil
		}
	}
	newWorkflow := func() func(*workflow.Context, struct{}) (string, error) {
		return func(wctx *workflow.Context, _ struct{}) (string, error) {
			return workflow.Execute[struct{}, string](wctx, "renewed", struct{}{})
		}
	}

	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: 200 * time.Millisecond,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, newActivity(), tasuki.WithName("renewed"))
	tasuki.RegisterWorkflow(w, newWorkflow(), tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("renew-through-commit-1"))
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(ctx)
	w.Start(parent)
	defer w.Shutdown(ctx)

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not enter CompleteActivity")
	}

	// Cancel the Start parent mid-commit: renewal would stop here without
	// the fix, while the detached commit keeps running.
	cancel()
	// Push the store clock past the original lease (t0+200ms) and give the
	// renewal loop several ticks (100ms period) to extend from the new
	// store time.
	mem.SetNow(t0.Add(10 * time.Second))
	time.Sleep(400 * time.Millisecond)

	// A peer must not find the task claimable mid-commit: renewal kept the
	// lease alive through the detached commit.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 0 {
		close(store.release)
		t.Fatalf("peer claimed %d tasks mid-commit, want 0 (renewal must cover the detached commit)", len(peer))
	}
	close(store.release)

	// The single detached commit must land: drain with a fresh worker.
	w2 := tasuki.NewWorker(store, tasuki.WorkerOptions{PollInterval: 5 * time.Millisecond})
	tasuki.RegisterActivity(w2, newActivity(), tasuki.WithName("renewed"))
	tasuki.RegisterWorkflow(w2, newWorkflow(), tasuki.WithName("WF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)
	deadline := time.Now().Add(8 * time.Second)
	for {
		info, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == tasuki.StatusCompleted {
			break
		}
		if info.Status == tasuki.StatusFailed {
			t.Fatal("workflow failed: detached commit was lost")
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow status=%q, want completed", info.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("activity executed %d times, want exactly once (no duplicate execution)", n)
	}
}
