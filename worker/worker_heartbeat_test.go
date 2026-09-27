package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/activity"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ActivityHeartbeatDetailsOnRetry(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var attempts atomic.Int32
	w := worker.NewWorker(b, worker.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Second,
	})
	worker.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		n := attempts.Add(1)
		info := activity.GetInfo(ctx)
		if info.ActivityName != "hb" || info.IdempotencyKey == "" {
			t.Errorf("bad info %+v", info)
		}
		if n == 1 {
			if err := activity.RecordHeartbeat(ctx, map[string]int{"step": 1}); err != nil {
				return "", err
			}
			return "", errors.New("transient")
		}
		var details struct {
			Step int `json:"step"`
		}
		if err := activity.GetHeartbeatDetails(ctx, &details); err != nil {
			t.Errorf("GetHeartbeatDetails: %v", err)
		}
		if details.Step != 1 {
			t.Errorf("step=%d want 1", details.Step)
		}
		return "ok", nil
	}, worker.WithName("hb"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "hb", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Millisecond}))
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("hb-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) && !b.HasRunnableTasks() {
			b.SetNow(next)
		} else if !b.HasRunnableTasks() {
			b.SetNow(b.Now().Add(10 * time.Millisecond))
		}
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			out, err := client.Result[string](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			if out != "ok" {
				t.Fatalf("got %q", out)
			}
			if attempts.Load() < 2 {
				t.Fatalf("attempts=%d", attempts.Load())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
