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

func TestWorker_ContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n < 3 {
			return 0, workflow.ContinueAsNew(wctx, n+1)
		}
		return n, nil
	}, worker.WithName("loop"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "loop", 0, client.WithID("cont-1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	candidates := []string{"cont-1", "cont-1~2", "cont-1~2~2", "cont-1~2~2~2"}
	for time.Now().Before(deadline) {
		for _, id := range candidates {
			info, err := c.Get(ctx, id)
			if err != nil {
				continue
			}
			if info.Status != "completed" {
				continue
			}
			h, err := client.Start(ctx, c, "loop", 0, client.WithID(id))
			if err != nil && !errors.Is(err, client.ErrAlreadyStarted) {
				t.Fatal(err)
			}
			out, err := client.Result[int](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			if out != 3 {
				t.Fatalf("got %d want 3 (id=%s)", out, id)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for continued run to complete")
}
