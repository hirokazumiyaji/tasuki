package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ClaimDueSchedules(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)

	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "scheduled", nil
	}, worker.WithName("cronJob"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	if err := c.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "every-hour", Cron: "0 * * * *", Workflow: "cronJob",
	}); err != nil {
		t.Fatal(err)
	}
	b.SetNow(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))

	deadline := time.Now().Add(3 * time.Second)
	instID := backend.ScheduleInstanceID("every-hour", time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, instID)
		if err == nil && info.Status == "completed" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("scheduled workflow did not complete")
}
