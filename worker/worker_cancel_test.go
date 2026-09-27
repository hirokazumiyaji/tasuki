package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_CancelWithCompensation(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	comp := false
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		comp = true
		return "refunded", nil
	}, worker.WithName("refund"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		err := workflow.Sleep(wctx, time.Hour)
		if errors.Is(err, workflow.ErrCanceled) {
			if _, err := workflow.Execute[struct{}, string](wctx, "refund", struct{}{}); err != nil {
				return "", err
			}
			return "", workflow.ErrCanceled
		}
		return "done", err
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	_, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("cancel-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Wait until suspended on sleep
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "cancel-1")
		if len(st.Journal) >= 2 { // started + timer
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Cancel(ctx, "cancel-1"); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, "cancel-1")
		if info.Status == "canceled" {
			if !comp {
				t.Fatal("compensation activity should have run")
			}
			return
		}
		if info.Status == "failed" || info.Status == "completed" {
			t.Fatalf("status=%s", info.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
