package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestClient_GetJournalAndList(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("act"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "act", struct{}{})
	}, worker.WithName("listWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "listWF", struct{}{}, client.WithID("list-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Result[string](ctx, h); err != nil {
		t.Fatal(err)
	}

	events, err := c.GetJournal(ctx, "list-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 || events[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("journal: %+v", events)
	}

	stuck, err := c.List(ctx, client.InstanceFilter{Status: client.StatusStuck})
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 0 {
		t.Fatalf("want no stuck, got %d", len(stuck))
	}
	done, err := c.List(ctx, client.InstanceFilter{Status: client.StatusCompleted, Name: "listWF"})
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 || done[0].ID != "list-1" {
		t.Fatalf("list completed: %+v", done)
	}
}
