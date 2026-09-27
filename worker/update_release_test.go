package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestUpdate_ReleasesSubscription is a regression test for the Update task
// subscription leak: Update must release its TaskNotifier subscription on
// every exit path without cancelling the caller's context.
func TestUpdate_ReleasesSubscription(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.SetUpdateHandler(wctx, "bump", func(wctx *workflow.Context, n int) (int, error) {
			return n + 1, nil
		})
		return "", workflow.Sleep(wctx, time.Hour)
	}, worker.WithName("updLeakWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "updLeakWF", struct{}{}, client.WithID("upd-leak-1")); err != nil {
		t.Fatal(err)
	}
	// Wait until the workflow is parked (handler registered).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "upd-leak-1")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	waitForSubs := func(want int) int {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if n := b.TaskSubCount(); n == want {
				return n
			}
			time.Sleep(time.Millisecond)
		}
		return b.TaskSubCount()
	}
	// The background worker loop holds one task subscription.
	if base := waitForSubs(1); base != 1 {
		t.Fatalf("worker baseline task subs = %d, want 1", base)
	}

	// Long-lived caller ctx: the Update must not accumulate subscribers.
	callCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, err := worker.Update[int, int](callCtx, w, "upd-leak-1", "bump", 41, worker.WithUpdateID("leak-u-0"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	// Allow the hub cleanup goroutine to run after the derived ctx is cancelled.
	if n := waitForSubs(1); n != 1 {
		t.Fatalf("task subs leaked after Update: %d, want baseline 1", n)
	}
	if callCtx.Err() != nil {
		t.Fatal("caller context must not be cancelled by Update")
	}
}
