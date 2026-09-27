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

// extendCountingBackend counts ExtendLease calls so tests can assert
// renewal started, kept going, or stopped.
type extendCountingBackend struct {
	backend.Backend
	extends atomic.Int32
}

func (b *extendCountingBackend) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	b.extends.Add(1)
	return b.Backend.ExtendLease(ctx, t, d)
}

func (b *extendCountingBackend) extendCount() int {
	return int(b.extends.Load())
}

// TestWorker_Round11_QueuedTurnKeepsLease is the regression test for
// round-11 P1 (start lease renewal before waiting on the instance
// actor): a claimed workflow task queued behind a long turn on its
// per-instance actor must still be renewed while queued. Without
// renewal the lease expires mid-queue and a peer reclaims it —
// concurrent execution of the same turn, including local side effects.
//
// The test holds the instance actor directly (simulating an old turn
// still holding it after a restart-after-timeout) and runs a tick that
// claims the instance's task: the claim queues on the actor past the
// lease. With the fix the pre-dispatch renewal keeps the lease and a
// peer probe finds nothing; without it the probe reclaims the task.
func TestWorker_Round11_QueuedTurnKeepsLease(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	w := NewWorker(mem, WorkerOptions{
		Queues:              []string{"default"},
		LeaseDuration:       400 * time.Millisecond, // 200ms renewal tick
		WorkerID:            "w1",
		WorkflowConcurrency: 2,
	})
	var runs atomic.Int32
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		runs.Add(1)
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(mem)
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID("round11-queued-1")); err != nil {
		t.Fatal(err)
	}

	// An old turn holds the per-instance actor past the lease.
	actor := w.actorFor("round11-queued-1")
	actor.mu.Lock()
	defer actor.mu.Unlock()

	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	// Let the claim land: the tick claims fast (in-memory backend)
	// then queues on the held actor.
	time.Sleep(300 * time.Millisecond)

	// Advance the store clock past the 400ms lease in small steps,
	// giving the 200ms renewal tick real time to fire between steps.
	// A covered claim stays ahead; an uncovered one goes stale.
	for i := 0; i < 8; i++ {
		time.Sleep(150 * time.Millisecond)
		mem.SetNow(t0.Add(time.Duration(100*(i+1)) * time.Millisecond))
	}

	// While still queued, the task must not be reclaimable by a peer.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 0 {
		actor.mu.Unlock()
		<-tickDone
		t.Fatalf("peer claimed %d tasks mid-queue, want 0 (queued turn must be renewed before dispatch)", len(peer))
	}

	// Release the actor: the queued turn runs exactly once and the
	// instance completes.
	actor.mu.Unlock()
	select {
	case <-tickDone:
	case <-time.After(8 * time.Second):
		t.Fatal("tick did not finish after the actor was released")
	}
	actor.mu.Lock()
	if n := runs.Load(); n != 1 {
		t.Fatalf("workflow executed %d times, want exactly once (no duplicate execution)", n)
	}
	info, err := c.Get(ctx, "round11-queued-1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusCompleted {
		t.Fatalf("status=%s, want completed", info.Status)
	}
}

// TestWorker_Round11_DispatchAbandonStopsRenewalAndReleases covers the
// other half of round-11 P1: when dispatch is abandoned (context
// canceled while queued on the actor), the claim's renewal must stop
// and the lease must be released for a prompt peer retry instead of
// running the turn late or holding it until expiry.
func TestWorker_Round11_DispatchAbandonStopsRenewalAndReleases(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &extendCountingBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		Queues:              []string{"default"},
		LeaseDuration:       400 * time.Millisecond, // 200ms renewal tick
		WorkerID:            "w1",
		WorkflowConcurrency: 2,
	})
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(store)
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID("round11-abandon-1")); err != nil {
		t.Fatal(err)
	}
	_ = c

	actor := w.actorFor("round11-abandon-1")
	actor.mu.Lock()
	defer actor.mu.Unlock()

	tickCtx, cancel := context.WithCancel(ctx)
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()
	time.Sleep(300 * time.Millisecond) // let the claim land and queue
	cancel()                           // abandon dispatch while queued
	select {
	case <-tickDone:
	case <-time.After(8 * time.Second):
		t.Fatal("tick did not return after dispatch was abandoned")
	}

	// The abandoned claim is released immediately: with the store clock
	// untouched, a peer reclaims it at once instead of waiting out the
	// lease.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1 (abandoned claim must be released promptly)", len(peer))
	}

	// Renewal stopped with the abandon: no ExtendLease issues after it.
	// (A live renewal would tick at least once in 400ms.)
	n := store.extendCount()
	time.Sleep(400 * time.Millisecond)
	if n2 := store.extendCount(); n2 != n {
		t.Fatalf("ExtendLease calls=%d->%d after abandon, want stable (renewal must stop when dispatch is abandoned)", n, n2)
	}
}

// TestWorker_Round11_DispatchWorkflowAbandonable unit-covers the actor
// primitive: lockCtx acquisition fails promptly on context cancel, and
// the fast path still runs the turn.
func TestWorker_Round11_DispatchWorkflowAbandonable(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{})

	// Free actor: runs the turn and reports true.
	ran := false
	if !w.dispatchWorkflow(context.Background(), "free-1", func() { ran = true }) {
		t.Fatal("dispatchWorkflow = false on a free actor, want true")
	}
	if !ran {
		t.Fatal("turn did not run on a free actor")
	}

	// Held actor + canceled context: abandons without running.
	actor := w.actorFor("held-1")
	actor.mu.Lock()
	defer actor.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- w.dispatchWorkflow(ctx, "held-1", func() {
			t.Error("abandoned dispatch must not run the turn")
		})
	}()
	time.Sleep(50 * time.Millisecond) // let it queue (1ms poll)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("dispatchWorkflow = true after cancel, want false (abandoned)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dispatchWorkflow did not abandon after context cancel")
	}
}
