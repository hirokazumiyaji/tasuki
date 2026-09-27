package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Panic once then succeed: retry must recover the worker process.
func TestWorker_ActivityPanicRetries(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var calls atomic.Int32
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return "ok", nil
	}, worker.WithName("panicky"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "panicky", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Millisecond}))
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("panic-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.SetNow(b.Now().Add(10 * time.Millisecond))
		info, _ := c.Get(ctx, h.ID())
		if info != nil && info.Status == "completed" {
			out, err := client.Result[string](ctx, h)
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
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		panic("always")
	}, worker.WithName("bad"))
	worker.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	}, worker.WithName("good"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		_, err := workflow.Execute[struct{}, string](wctx, "bad", struct{}{},
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Millisecond}))
		if err != nil {
			return "", err
		}
		return "done", nil
	}, worker.WithName("WF"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.Execute[int, int](wctx, "good", n)
	}, worker.WithName("GOOD"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	hbad, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("panic-2"))
	if err != nil {
		t.Fatal(err)
	}
	hgood, err := client.Start(ctx, c, "GOOD", 41, client.WithID("panic-good"))
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
			_, rerr := client.Result[string](rctx, hbad)
			cancel()
			if rerr == nil || !errors.Is(rerr, client.ErrFailed) {
				t.Fatalf("bad Result err=%v", rerr)
			}
			got, err := client.Result[int](ctx, hgood)
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
