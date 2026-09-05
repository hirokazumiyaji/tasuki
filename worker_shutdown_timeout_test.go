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

// fakeBlockingBackend blocks ReleaseLease until ctx ends.
type fakeBlockingBackend struct {
	backend.Backend
	releaseCh chan struct{}
}

func (f *fakeBlockingBackend) ReleaseLease(ctx context.Context, id int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.releaseCh:
		return f.Backend.ReleaseLease(context.Background(), id)
	}
}

func TestWorker_ShutdownLeaseReleaseTimeout(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	fake := &fakeBlockingBackend{Backend: mem, releaseCh: make(chan struct{})}
	w := tasuki.NewWorker(fake, tasuki.WorkerOptions{
		PollInterval:           time.Millisecond,
		ShutdownReleaseTimeout: 50 * time.Millisecond,
	})
	// Pretend an in-flight task by starting a worker loop with a claimed task.
	// Simpler: use PollOnce path with a real task, then block release.
	b := mem
	b.SetNow(time.Now().UTC())
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "x", nil
	}, tasuki.WithName("slow"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "slow", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(fake)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-timeout-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Shutdown(shCtx)
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("Shutdown took too long: %v", elapsed)
	}
	close(fake.releaseCh)
}

func TestWorker_LongActivityDoesNotBlockTimer(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	block := make(chan struct{})
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:        time.Millisecond,
		ActivityConcurrency: 1,
		WorkflowConcurrency: 1,
		ClaimLimit:          10,
	})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		<-block
		return "slow-done", nil
	}, tasuki.WithName("blocker"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		v, err := workflow.Execute[struct{}, string](wctx, "blocker", struct{}{})
		if err != nil {
			return "", err
		}
		return v, nil
	}, tasuki.WithName("BLOCK"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		if err := workflow.Sleep(wctx, time.Millisecond); err != nil {
			return "", err
		}
		return "timer-done", nil
	}, tasuki.WithName("TIMER"))
	w.Start(ctx)
	defer func() {
		close(block)
		w.Shutdown(ctx)
	}()
	c := tasuki.NewClient(b)
	hblock, err := tasuki.Start(ctx, c, "BLOCK", struct{}{}, tasuki.WithID("block-1"))
	if err != nil {
		t.Fatal(err)
	}
	_ = hblock
	htimer, err := tasuki.Start(ctx, c, "TIMER", struct{}{}, tasuki.WithID("timer-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Timer workflow must complete even while blocker activity is stuck.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, "timer-1")
		if info != nil && info.Status == "completed" {
			out, err := tasuki.Result[string](ctx, htimer)
			if err != nil {
				t.Fatal(err)
			}
			if out != "timer-done" {
				t.Fatalf("got %q", out)
			}
			return
		}
		// Advance virtual timer clock.
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) {
			b.SetNow(next)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timer did not progress while activity blocked")
}

// TestWorker_ShutdownWaitsForActivities verifies Shutdown waits for running
// activities within its grace period instead of releasing their leases
// immediately (which would let peers duplicate the execution).
func TestWorker_ShutdownWaitsForActivities(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())
	var calls atomic.Int32
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return "done", nil
	}, tasuki.WithName("quick"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "quick", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-wait-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	// Wait until the activity is claimed and running.
	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if calls.Load() == 0 {
		t.Fatal("activity did not start")
	}
	start := time.Now()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Shutdown(shCtx); err != nil {
		t.Fatal(err)
	}
	// Shutdown must have waited for the activity, not released it early.
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("Shutdown returned too early (%v): activity lease released before completion", elapsed)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("activity executed %d times, want exactly once", n)
	}
	// The activity task was consumed (not lease-released for a duplicate):
	// no claimable activity task may remain.
	leftover, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10, Lease: time.Second, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(leftover) != 0 {
		t.Fatalf("%d activity tasks leaked for duplicate execution", len(leftover))
	}
	// Follow-up workflow work resumes on the next worker start; the instance
	// must not have failed.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status == tasuki.StatusFailed {
		t.Fatalf("status=%q after graceful shutdown", info.Status)
	}
}
