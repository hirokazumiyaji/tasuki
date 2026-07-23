package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestClient_GetJournalAndList(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, tasuki.WithName("act"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "act", struct{}{})
	}, tasuki.WithName("listWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "listWF", struct{}{}, tasuki.WithID("list-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasuki.Result[string](ctx, h); err != nil {
		t.Fatal(err)
	}

	events, err := c.GetJournal(ctx, "list-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 || events[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("journal: %+v", events)
	}

	stuck, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusStuck})
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 0 {
		t.Fatalf("want no stuck, got %d", len(stuck))
	}
	done, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusCompleted, Name: "listWF"})
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 || done[0].ID != "list-1" {
		t.Fatalf("list completed: %+v", done)
	}
}
