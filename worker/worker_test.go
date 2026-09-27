package worker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ReplayResumesFromPartialJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	step1 := func(ctx context.Context, _ struct{}) (string, error) { return "a", nil }
	step2 := func(ctx context.Context, _ struct{}) (string, error) { return "b", nil }

	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, step1, worker.WithName("step1"))
	worker.RegisterActivity(w, step2, worker.WithName("step2"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		a, err := workflow.Execute[struct{}, string](wctx, "step1", struct{}{})
		if err != nil {
			return "", err
		}
		b2, err := workflow.Execute[struct{}, string](wctx, "step2", struct{}{})
		if err != nil {
			return "", err
		}
		return a + b2, nil
	}, worker.WithName("WF"))

	c := client.NewClient(b)
	_, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("id-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	defer w.Shutdown(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "id-1")
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == "completed" {
			var out string
			if err := json.Unmarshal(info.Result, &out); err != nil {
				t.Fatal(err)
			}
			if out != "ab" {
				t.Fatalf("got %q", out)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for completion")
}
