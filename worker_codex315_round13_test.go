package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorkflow_CanceledTickSkipsPendingFlush is the regression test for the
// round-13 P1 finding (skip pending commits after the tick is canceled):
// with WorkflowConcurrency > 1 a completed turn sits pending while wg.Wait()
// waits for a slower sibling, and Shutdown cancels the tick context before
// the unconditional flush. Context-aware backends reject the canceled
// flush, but the memory backend ignores the context and persists the
// advancement — though documented behavior says canceled turns are
// abandoned and released for a peer retry.
//
// Without the fix the fast instance is completed by the canceled flush;
// with it the pending advancement is disposed (ownership-gated,
// kind-routed release) so the instance stays running and its task is
// immediately reclaimable by a peer.
func TestWorkflow_CanceledTickSkipsPendingFlush(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	var fastCalls, slowCalls atomic.Int32
	fastLocal := func(context.Context, int) (int, error) {
		fastCalls.Add(1)
		return 42, nil
	}
	slowLocal := func(actCtx context.Context, n int) (int, error) {
		if slowCalls.Add(1) == 1 {
			// First attempt blocks until the tick is canceled.
			<-actCtx.Done()
			return 0, actCtx.Err()
		}
		return n * 2, nil
	}
	fastWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "fastLocal", n)
	}
	slowWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "slowLocal", n)
	}

	w1 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:        5 * time.Millisecond,
		LeaseDuration:       time.Minute,
		WorkflowConcurrency: 2,
		ClaimLimit:          2,
		WorkerID:            "w1",
	})
	tasuki.RegisterActivity(w1, fastLocal, tasuki.WithName("fastLocal"))
	tasuki.RegisterActivity(w1, slowLocal, tasuki.WithName("slowLocal"))
	tasuki.RegisterWorkflow(w1, fastWF, tasuki.WithName("cancelFastWF"))
	tasuki.RegisterWorkflow(w1, slowWF, tasuki.WithName("cancelSlowWF"))

	c := tasuki.NewClient(b)
	// Start both instances before the worker so one tick claims them
	// into a single batch: the fast turn completes while the slow turn
	// keeps wg.Wait() blocked.
	fh, err := tasuki.Start(ctx, c, "cancelFastWF", 21, tasuki.WithID("cancel-flush-fast"))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := tasuki.Start(ctx, c, "cancelSlowWF", 7, tasuki.WithID("cancel-flush-slow"))
	if err != nil {
		t.Fatal(err)
	}
	_ = sh
	w1.Start(ctx)

	// Wait until both turns are running, then let the fast turn finish
	// into pending while the slow turn stays blocked in wg.Wait().
	deadline := time.Now().Add(5 * time.Second)
	for fastCalls.Load() < 1 || slowCalls.Load() < 1 {
		if time.Now().After(deadline) {
			_ = w1.Shutdown(ctx)
			t.Fatalf("turns did not start (fast=%d slow=%d)", fastCalls.Load(), slowCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w1.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := fastCalls.Load(); n != 1 {
		t.Fatalf("fast local activity executed %d times, want 1 (no duplicate execution during shutdown)", n)
	}
	if n := slowCalls.Load(); n != 1 {
		t.Fatalf("slow local activity executed %d times, want 1", n)
	}

	// The canceled flush must not persist the fast advancement: the
	// instance stays running instead of completing under shutdown.
	info, err := c.Get(ctx, fh.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info == nil {
		t.Fatal("missing fast instance info after shutdown")
	}
	if info.Status != "running" {
		t.Fatalf("fast status=%s, want running (canceled tick must not flush pending commits)", info.Status)
	}

	// The disposed pending task is released promptly: a peer reclaims
	// the fast task at once instead of waiting out the lease.
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range peer {
		if task.InstanceID == fh.ID() {
			found = true
		}
	}
	if !found {
		t.Fatalf("peer did not reclaim the fast task (claimed %d tasks, want the canceled pending turn among them)", len(peer))
	}
}
