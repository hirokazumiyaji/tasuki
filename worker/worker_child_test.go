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

func TestWorker_ChildWorkflow(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, worker.WithName("inc"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "inc", n)
	}, worker.WithName("child"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteChild[int, int](wctx, "child", n)
	}, worker.WithName("parent"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "parent", 1, client.WithID("parent-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Result[int](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("got %d", out)
	}
}
