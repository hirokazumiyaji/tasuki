package tasuki_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ReplayResumesFromPartialJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	step1 := func(ctx context.Context, _ struct{}) (string, error) { return "a", nil }
	step2 := func(ctx context.Context, _ struct{}) (string, error) { return "b", nil }

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, step1, tasuki.WithName("step1"))
	tasuki.RegisterActivity(w, step2, tasuki.WithName("step2"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		a, err := workflow.Execute[struct{}, string](wctx, "step1", struct{}{})
		if err != nil {
			return "", err
		}
		b2, err := workflow.Execute[struct{}, string](wctx, "step2", struct{}{})
		if err != nil {
			return "", err
		}
		return a + b2, nil
	}, tasuki.WithName("WF"))

	c := tasuki.NewClient(b)
	_, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("id-1"))
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
