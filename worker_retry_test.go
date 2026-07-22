package tasuki_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_RetriesRetryableActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var attempts atomic.Int32
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		n := attempts.Add(1)
		if n < 3 {
			return "", errors.New("transient")
		}
		return "ok", nil
	}, tasuki.WithName("flaky"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "flaky", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Millisecond}))
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("retry-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Drive virtual time for backoff
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) && !b.HasRunnableTasks() {
			b.SetNow(next)
		} else if !b.HasRunnableTasks() {
			// activity backoff uses visible_at, not timers — advance clock a bit
			b.SetNow(b.Now().Add(10 * time.Millisecond))
		}
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			out, err := tasuki.Result[string](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			if out != "ok" {
				t.Fatalf("got %q", out)
			}
			if attempts.Load() != 3 {
				t.Fatalf("attempts=%d", attempts.Load())
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timeout")
}

func TestWorker_NonRetryable(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var attempts atomic.Int32
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		attempts.Add(1)
		return "", tasuki.NonRetryable(errors.New("bad input"))
	}, tasuki.WithName("bad"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "bad", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5}))
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("nr-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			t.Fatal("should not complete successfully")
		}
		if info != nil && info.Status == "failed" {
			if attempts.Load() != 1 {
				t.Fatalf("attempts=%d want 1", attempts.Load())
			}
			return
		}
		// workflow fails when activity_failed returned
		out, err := tasuki.Result[string](context.Background(), h)
		_ = out
		if err != nil && attempts.Load() >= 1 {
			if attempts.Load() != 1 {
				t.Fatalf("attempts=%d", attempts.Load())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
