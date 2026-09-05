package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// fakeBlockingBackend blocks ReleaseLease until ctx ends.
type fakeBlockingBackend struct {
	backend.Backend
	releaseCh chan struct{}
}

func (f *fakeBlockingBackend) ReleaseLease(ctx context.Context, id int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.releaseCh:
		return f.Backend.ReleaseLease(context.Background(), id)
	}
}

func TestWorker_ShutdownLeaseReleaseTimeout(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	fake := &fakeBlockingBackend{Backend: mem, releaseCh: make(chan struct{})}
	w := tasuki.NewWorker(fake, tasuki.WorkerOptions{
		PollInterval:           time.Millisecond,
		ShutdownReleaseTimeout: 50 * time.Millisecond,
	})
	// Pretend an in-flight task by starting a worker loop with a claimed task.
	// Simpler: use PollOnce path with a real task, then block release.
	b := mem
	b.SetNow(time.Now().UTC())
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "x", nil
	}, tasuki.WithName("slow"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "slow", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(fake)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-timeout-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Shutdown(shCtx)
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("Shutdown took too long: %v", elapsed)
	}
	close(fake.releaseCh)
}

func TestWorker_LongActivityDoesNotBlockTimer(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	block := make(chan struct{})
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:        time.Millisecond,
		ActivityConcurrency: 1,
		WorkflowConcurrency: 1,
		ClaimLimit:          10,
	})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		<-block
		return "slow-done", nil
	}, tasuki.WithName("blocker"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		v, err := workflow.Execute[struct{}, string](wctx, "blocker", struct{}{})
		if err != nil {
			return "", err
		}
		return v, nil
	}, tasuki.WithName("BLOCK"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		if err := workflow.Sleep(wctx, time.Millisecond); err != nil {
			return "", err
		}
		return "timer-done", nil
	}, tasuki.WithName("TIMER"))
	w.Start(ctx)
	defer func() {
		close(block)
		w.Shutdown(ctx)
	}()
	c := tasuki.NewClient(b)
	hblock, err := tasuki.Start(ctx, c, "BLOCK", struct{}{}, tasuki.WithID("block-1"))
	if err != nil {
		t.Fatal(err)
	}
	_ = hblock
	htimer, err := tasuki.Start(ctx, c, "TIMER", struct{}{}, tasuki.WithID("timer-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Timer workflow must complete even while blocker activity is stuck.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, "timer-1")
		if info != nil && info.Status == "completed" {
			out, err := tasuki.Result[string](ctx, htimer)
			if err != nil {
				t.Fatal(err)
			}
			if out != "timer-done" {
				t.Fatalf("got %q", out)
			}
			return
		}
		// Advance virtual timer clock.
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) {
			b.SetNow(next)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timer did not progress while activity blocked")
}
