package tasuki_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
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

// TestClient_TerminateFollowsContinueAsNew guards the tail-resolution order:
// the live tail must be resolved before the head is terminated, otherwise
// the head's "continued" status is overwritten and the child keeps running.
func TestClient_TerminateFollowsContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n == 0 {
			return 0, workflow.ContinueAsNew(wctx, 1)
		}
		// Park the tail on a signal so it stays running.
		s, err := workflow.ReceiveSignal[string](wctx, "go")
		if err != nil {
			return 0, err
		}
		return len(s), nil
	}, tasuki.WithName("park"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "park", 0, tasuki.WithID("term-chain-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the head continued and the tail is running.
	tailID := ""
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		head, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if head.Status == tasuki.StatusContinued {
			j, err := b.GetJournal(ctx, h.ID(), 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range j {
				if e.Type == journal.TypeContinuedAsNew {
					tailID = h.ID() + "~" + strconv.FormatInt(e.Seq, 10)
				}
			}
			if tailID != "" {
				if tail, err := c.Get(ctx, tailID); err == nil && tail.Status == tasuki.StatusRunning {
					break
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if tailID == "" {
		t.Fatal("chain did not continue")
	}
	if err := c.Terminate(ctx, h.ID()); err != nil {
		t.Fatal(err)
	}
	if tail, err := c.Get(ctx, tailID); err != nil {
		t.Fatal(err)
	} else if tail.Status != tasuki.StatusTerminated {
		t.Fatalf("tail status=%q, want terminated", tail.Status)
	}
}

// TestClient_CancelFollowsContinueAsNew verifies Cancel reaches the live
// tail: a parked tail observes cancellation instead of hanging.
func TestClient_CancelFollowsContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n == 0 {
			return 0, workflow.ContinueAsNew(wctx, 1)
		}
		if _, err := workflow.ReceiveSignal[string](wctx, "go"); err != nil {
			return 0, err
		}
		return 1, nil
	}, tasuki.WithName("park"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "park", 0, tasuki.WithID("cancel-chain-1"))
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the tail to park, then cancel via the original handle.
	deadline := time.Now().Add(3 * time.Second)
	parked := false
	for time.Now().Before(deadline) {
		head, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if head.Status == tasuki.StatusContinued {
			parked = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !parked {
		t.Fatal("chain did not continue")
	}
	time.Sleep(50 * time.Millisecond) // let the tail park on its signal
	if err := c.Cancel(ctx, h.ID()); err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := tasuki.Result[int](rctx, h); err == nil {
		t.Fatal("expected cancellation error")
	}
}
