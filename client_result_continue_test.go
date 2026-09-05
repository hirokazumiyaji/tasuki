package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestResult_FollowsContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n < 3 {
			return 0, workflow.ContinueAsNew(wctx, n+1)
		}
		return n, nil
	}, tasuki.WithName("loop"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "loop", 0, tasuki.WithID("follow-1"))
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := tasuki.Result[int](rctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != 3 {
		t.Fatalf("got %d want 3", out)
	}
}

func TestResult_ReleasesSubscription(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return n, nil
	}, tasuki.WithName("echo"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	// Long-lived caller ctx: repeated Results must not accumulate subscribers.
	callCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 5; i++ {
		id := "leak-" + string(rune('a'+i))
		h, err := tasuki.Start(ctx, c, "echo", i, tasuki.WithID(id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tasuki.Result[int](callCtx, h); err != nil {
			t.Fatal(err)
		}
		// Allow hub cleanup goroutine to run.
		deadline := time.Now().Add(time.Second)
		for b.TerminalSubCount() != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if n := b.TerminalSubCount(); n != 0 {
			t.Fatalf("iter %d: terminal subs leaked: %d", i, n)
		}
	}
	if callCtx.Err() != nil {
		t.Fatal("caller context must not be cancelled by Result")
	}
}

func TestResult_CancelContext(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Hour})
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "missing", 0, tasuki.WithID("cancel-1"))
	if err != nil {
		t.Fatal(err)
	}
	_ = w
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tasuki.Result[int](cctx, h); err == nil {
		t.Fatal("expected ctx error")
	}
}
