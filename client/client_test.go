package client_test

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

func TestHandle_Result(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, worker.WithName("inc"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "inc", n)
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "WF", 41, client.WithID("res-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Result[int](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d", out)
	}
}

func TestClient_Terminate(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		_ = workflow.Sleep(wctx, 24*time.Hour)
		return "done", nil
	}, worker.WithName("sleepy"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "sleepy", struct{}{}, client.WithID("term-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Let it suspend on sleep
	time.Sleep(20 * time.Millisecond)
	if err := c.Terminate(ctx, "term-1"); err != nil {
		t.Fatal(err)
	}
	info, err := c.Get(ctx, "term-1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "terminated" {
		t.Fatalf("status=%s", info.Status)
	}
	_, err = client.Result[string](ctx, h)
	if !errors.Is(err, client.ErrTerminated) {
		t.Fatalf("want ErrTerminated, got %v", err)
	}
}
