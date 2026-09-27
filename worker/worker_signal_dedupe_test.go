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

func TestWorker_SignalDedupeDeliveredOnce(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
		n, err := workflow.ReceiveSignal[int](wctx, "tick")
		if err != nil {
			return 0, err
		}
		return n, nil
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("sig-dedupe-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, h.ID())
		if st != nil && len(st.Journal) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := c.Signal(ctx, h.ID(), "tick", 7, client.WithDedupeID("once")); err != nil {
		t.Fatal(err)
	}
	if err := c.Signal(ctx, h.ID(), "tick", 8, client.WithDedupeID("once")); err != nil {
		t.Fatal(err)
	}

	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			out, err := client.Result[int](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			if out != 7 {
				t.Fatalf("result=%d want 7", out)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
