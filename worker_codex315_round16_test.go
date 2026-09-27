package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorkflow_ShutdownUnboundedLocalCancelsBeforeFurtherCalls is the
// regression test for round-16 P2 (propagate shutdown after unbounded
// local calls): with the default LocalActivityTimeout == 0, Shutdown
// cancels runCtx while a local activity runs, but the activity observes
// the cancellation yet returns a value. The timeout-disabled branch used
// to forward that value without a runCtx.Err() check (the check lived
// only in acceptLocalResult for the timeout-enabled branch), so the
// workflow accepted it and invoked FURTHER local activities — with side
// effects — before handleWorkflow's post-return guard abandoned the turn.
//
// With the fix, cancellation wins over the returned value regardless of
// the timeout setting: the first post-shutdown ExecuteLocal surfaces the
// turn cancellation, the workflow returns early, and no further local
// activity runs. Without the fix the second local runs during the
// canceled attempt.
func TestWorkflow_ShutdownUnboundedLocalCancelsBeforeFurtherCalls(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	started := make(chan struct{}, 1)
	var firstCalls, secondCalls atomic.Int32
	firstLocal := func(actCtx context.Context, _ struct{}) (string, error) {
		if firstCalls.Add(1) == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			// Observe shutdown but swallow it: return a value instead
			// of the cancellation error.
			<-actCtx.Done()
			return "shutdown-value", nil
		}
		return "recovered", nil
	}
	secondLocal := func(context.Context, struct{}) (string, error) {
		secondCalls.Add(1)
		return "second", nil
	}
	chainWF := func(wctx *workflow.Context, _ struct{}) (string, error) {
		v1, err := workflow.ExecuteLocal[struct{}, string](wctx, "firstLocal", struct{}{})
		if err != nil {
			return "", err
		}
		v2, err := workflow.ExecuteLocal[struct{}, string](wctx, "secondLocal", struct{}{})
		if err != nil {
			return "", err
		}
		return v1 + "/" + v2, nil
	}

	// Default options: LocalActivityTimeout == 0 (unbounded).
	w1 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w1, firstLocal, tasuki.WithName("firstLocal"))
	tasuki.RegisterActivity(w1, secondLocal, tasuki.WithName("secondLocal"))
	tasuki.RegisterWorkflow(w1, chainWF, tasuki.WithName("chainWF"))
	w1.Start(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "chainWF", struct{}{}, tasuki.WithID("shutdown-unbounded-chain-1"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local activity did not start")
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w1.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := firstCalls.Load(); n != 1 {
		t.Fatalf("firstLocal calls=%d, want 1 (no duplicate execution during shutdown)", n)
	}
	if n := secondCalls.Load(); n != 0 {
		t.Fatalf("secondLocal calls=%d, want 0 (canceled unbounded result must not let the workflow run further locals)", n)
	}

	// The canceled turn must be abandoned, not committed with the
	// post-shutdown value.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info == nil {
		t.Fatal("missing instance info after shutdown")
	}
	if info.Status != "running" {
		t.Fatalf("status=%s, want running (shutdown must abandon the turn, not commit it)", info.Status)
	}

	// A peer worker retries and completes the chain normally.
	w2 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w2",
	})
	tasuki.RegisterActivity(w2, firstLocal, tasuki.WithName("firstLocal"))
	tasuki.RegisterActivity(w2, secondLocal, tasuki.WithName("secondLocal"))
	tasuki.RegisterWorkflow(w2, chainWF, tasuki.WithName("chainWF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)

	resCtx, resCancel := context.WithTimeout(ctx, 10*time.Second)
	defer resCancel()
	got, err := tasuki.Result[string](resCtx, h)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != "recovered/second" {
		t.Fatalf("got %q want %q", got, "recovered/second")
	}
	if n := firstCalls.Load(); n != 2 {
		t.Fatalf("firstLocal calls=%d, want 2 (canceled attempt + peer retry)", n)
	}
	if n := secondCalls.Load(); n != 1 {
		t.Fatalf("secondLocal calls=%d, want 1 (peer retry only)", n)
	}
}
