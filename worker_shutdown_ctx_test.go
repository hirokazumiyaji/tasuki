package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// ctxCheckingBackend simulates pgx-style stores that reject work on a
// canceled context (memory itself ignores ctx and cannot catch the bug).
type ctxCheckingBackend struct {
	backend.Backend
	canceledCommits atomic.Int32
	canceledRetries atomic.Int32
}

func (b *ctxCheckingBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	if ctx.Err() != nil {
		b.canceledCommits.Add(1)
		return ctx.Err()
	}
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

func (b *ctxCheckingBackend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	if ctx.Err() != nil {
		b.canceledRetries.Add(1)
		return ctx.Err()
	}
	return b.Backend.RetryActivity(ctx, taskID, delay)
}

// TestWorker_ShutdownCommitsActivityWithLiveContext is the regression test for
// issue #282: Shutdown must not commit within-grace activity results with an
// already-canceled context.
func TestWorker_ShutdownCommitsActivityWithLiveContext(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &ctxCheckingBackend{Backend: mem}

	started := make(chan struct{})
	var startOnce atomic.Bool
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		if startOnce.CompareAndSwap(false, true) {
			close(started)
		}
		// Finish within the shutdown grace while ignoring ctx cancellation.
		time.Sleep(100 * time.Millisecond)
		return "done", nil
	}, tasuki.WithName("quick"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "quick", struct{}{})
	}, tasuki.WithName("WF"))

	c := tasuki.NewClient(store)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-ctx-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("activity did not start")
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Shutdown(shCtx); err != nil {
		t.Fatal(err)
	}

	if n := store.canceledCommits.Load(); n != 0 {
		t.Fatalf("canceledCommits=%d, want 0 (result committed with canceled ctx)", n)
	}
	if n := store.canceledRetries.Load(); n != 0 {
		t.Fatalf("canceledRetries=%d, want 0", n)
	}

	// Drain the follow-up workflow task so the completion becomes visible.
	w2 := tasuki.NewWorker(store, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w2, func(ctx context.Context, _ struct{}) (string, error) {
		return "done", nil
	}, tasuki.WithName("quick"))
	tasuki.RegisterWorkflow(w2, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "quick", struct{}{})
	}, tasuki.WithName("WF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == tasuki.StatusCompleted {
			break
		}
		if info.Status == tasuki.StatusFailed {
			t.Fatalf("workflow failed after graceful shutdown")
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow status=%q, want completed (canceledCommits=%d)", info.Status, store.canceledCommits.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	out, err := tasuki.Result[string](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != "done" {
		t.Fatalf("result=%q, want %q", out, "done")
	}
}

// TestWorker_ShutdownCancelDoesNotConsumeAttempt ensures a Shutdown-caused
// context.Canceled is released (not recorded as retry/failure and not
// consuming an attempt via a timeout failure).
func TestWorker_ShutdownCancelDoesNotConsumeAttempt(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &ctxCheckingBackend{Backend: mem}

	var calls atomic.Int32
	started := make(chan struct{})
	var startOnce atomic.Bool
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			if startOnce.CompareAndSwap(false, true) {
				close(started)
			}
			// Respect shutdown: abort only when the execution ctx is cut
			// (after the grace), then return Canceled.
			<-actCtx.Done()
			return "", actCtx.Err()
		}
		return "recovered", nil
	}, tasuki.WithName("respectful"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "respectful", struct{}{}, workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 1}))
	}, tasuki.WithName("WF"))

	c := tasuki.NewClient(store)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-ctx-2"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("activity did not start")
	}

	// Short grace: the first attempt outlives it and must be released, not failed.
	shCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Shutdown(shCtx)

	if n := store.canceledCommits.Load(); n != 0 {
		t.Fatalf("canceledCommits=%d, want 0", n)
	}
	if n := store.canceledRetries.Load(); n != 0 {
		t.Fatalf("canceledRetries=%d, want 0 (shutdown cancel must not retry)", n)
	}

	// A fresh worker must be able to recover the workflow to completion.
	w2 := tasuki.NewWorker(store, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w2, func(actCtx context.Context, _ struct{}) (string, error) {
		calls.Add(1)
		return "recovered", nil
	}, tasuki.WithName("respectful"))
	tasuki.RegisterWorkflow(w2, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "respectful", struct{}{}, workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 1}))
	}, tasuki.WithName("WF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == tasuki.StatusCompleted {
			break
		}
		if info.Status == tasuki.StatusFailed {
			t.Fatalf("workflow failed: shutdown cancel consumed the single attempt")
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow status=%q, want completed", info.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
