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

// Panic once then succeed: retry must recover the worker process.
func TestWorker_ActivityPanicRetries(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var calls atomic.Int32
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return "ok", nil
	}, tasuki.WithName("panicky"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "panicky", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Millisecond}))
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("panic-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.SetNow(b.Now().Add(10 * time.Millisecond))
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			out, err := tasuki.Result[string](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			if out != "ok" || calls.Load() < 2 {
				t.Fatalf("out=%q calls=%d", out, calls.Load())
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timeout: panic did not retry to success")
}

// Continuous panic hits MaxAttempts and other activities keep flowing.
func TestWorker_ActivityPanicMaxAttempts(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		panic("always")
	}, tasuki.WithName("bad"))
	tasuki.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, tasuki.WithName("good"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		_, err := workflow.Execute[struct{}, string](wctx, "bad", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Millisecond}))
		if err != nil {
			return "", err
		}
		return "done", nil
	}, tasuki.WithName("WF"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "good", n)
	}, tasuki.WithName("GOOD"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	hbad, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("panic-2"))
	if err != nil {
		t.Fatal(err)
	}
	hgood, err := tasuki.Start(ctx, c, "GOOD", 41, tasuki.WithID("panic-good"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var badDone, goodDone bool
	for time.Now().Before(deadline) {
		b.SetNow(b.Now().Add(10 * time.Millisecond))
		if !badDone {
			if info, _ := c.Get(ctx, hbad.ID()); info != nil && info.Status == "failed" {
				badDone = true
			}
		}
		if !goodDone {
			if info, _ := c.Get(ctx, hgood.ID()); info != nil && info.Status == "completed" {
				goodDone = true
			}
		}
		if badDone && goodDone {
			// Verify Result surfaces the failure and good returns 42.
			rctx, cancel := context.WithTimeout(ctx, time.Second)
			_, rerr := tasuki.Result[string](rctx, hbad)
			cancel()
			if rerr == nil || !errors.Is(rerr, tasuki.ErrFailed) {
				t.Fatalf("bad Result err=%v", rerr)
			}
			got, err := tasuki.Result[int](ctx, hgood)
			if err != nil || got != 42 {
				t.Fatalf("good got=%d err=%v", got, err)
			}
			if calls, _ := c.Get(ctx, hbad.ID()); calls == nil {
				t.Fatal("missing bad instance")
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout badDone=%v goodDone=%v", badDone, goodDone)
}
