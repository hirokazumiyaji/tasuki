package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ChildWorkflow(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, tasuki.WithName("inc"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "inc", n)
	}, tasuki.WithName("child"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteChild[int, int](wctx, "child", n)
	}, tasuki.WithName("parent"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "parent", 1, tasuki.WithID("parent-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := tasuki.Result[int](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("got %d", out)
	}
}
