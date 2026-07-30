package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func coverNamedWorkflow(ctx *workflow.Context, n int) (int, error) {
	return n + 1, nil
}

func TestRegisterWorkflow_DefaultNameAndAutoID(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, coverNamedWorkflow) // no WithName → funcName
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "coverNamedWorkflow", 1) // no WithID → newID
	if err != nil {
		t.Fatal(err)
	}
	if h.ID() == "" {
		t.Fatal("empty id")
	}
	out, err := tasuki.Result[int](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("got %d", out)
	}
}
