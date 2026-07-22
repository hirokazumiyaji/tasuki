package tasuki_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestHandle_Result(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, tasuki.WithName("inc"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "inc", n)
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", 41, tasuki.WithID("res-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := tasuki.Result[int](ctx, h)
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
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		_ = workflow.Sleep(wctx, 24*time.Hour)
		return "done", nil
	}, tasuki.WithName("sleepy"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "sleepy", struct{}{}, tasuki.WithID("term-1"))
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
	_, err = tasuki.Result[string](ctx, h)
	if !errors.Is(err, tasuki.ErrTerminated) {
		t.Fatalf("want ErrTerminated, got %v", err)
	}
}
