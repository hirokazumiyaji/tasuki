package tasuki_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestActivity_StartToCloseTimeout_FailsAttempt(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-actCtx.Done():
			return "", actCtx.Err()
		case <-time.After(200 * time.Millisecond):
			return "too-slow", nil
		}
	}, tasuki.WithName("slow"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "slow", struct{}{},
			workflow.WithStartToCloseTimeout(20*time.Millisecond),
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 1}),
		)
	}, tasuki.WithName("stcWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "stcWF", struct{}{}, tasuki.WithID("stc-1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = tasuki.Result[string](ctx, h)
	if err == nil {
		t.Fatal("expected timeout failure")
	}
	if !strings.Contains(err.Error(), "activity start-to-close timeout") {
		t.Fatalf("err=%v", err)
	}
}

func TestActivity_StartToCloseTimeout_CanRetry(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
	})
	var attempts atomic.Int32
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		n := attempts.Add(1)
		if n == 1 {
			select {
			case <-actCtx.Done():
				return "", actCtx.Err()
			case <-time.After(200 * time.Millisecond):
				return "slow", nil
			}
		}
		return "ok", nil
	}, tasuki.WithName("flaky"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "flaky", struct{}{},
			workflow.WithStartToCloseTimeout(20*time.Millisecond),
			workflow.WithRetry(workflow.RetryPolicy{
				InitialInterval: time.Millisecond,
				MaxAttempts:     3,
			}),
		)
	}, tasuki.WithName("stcRetryWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "stcRetryWF", struct{}{}, tasuki.WithID("stc-retry-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !b.HasRunnableTasks() {
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
			if attempts.Load() < 2 {
				t.Fatalf("attempts=%d", attempts.Load())
			}
			return
		}
		if info != nil && (info.Status == "failed" || info.Status == "stuck") {
			t.Fatalf("status=%s", info.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
