package client_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestResult_FollowsContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n < 3 {
			return 0, workflow.ContinueAsNew(wctx, n+1)
		}
		return n, nil
	}, worker.WithName("loop"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "loop", 0, client.WithID("follow-1"))
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := client.Result[int](rctx, h)
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
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return n, nil
	}, worker.WithName("echo"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	// Long-lived caller ctx: repeated Results must not accumulate subscribers.
	callCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 5; i++ {
		id := "leak-" + string(rune('a'+i))
		h, err := client.Start(ctx, c, "echo", i, client.WithID(id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Result[int](callCtx, h); err != nil {
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
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Hour})
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "missing", 0, client.WithID("cancel-1"))
	if err != nil {
		t.Fatal(err)
	}
	_ = w
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Result[int](cctx, h); err == nil {
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
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n == 0 {
			return 0, workflow.ContinueAsNew(wctx, 1)
		}
		// Park the tail on a signal so it stays running.
		s, err := workflow.ReceiveSignal[string](wctx, "go")
		if err != nil {
			return 0, err
		}
		return len(s), nil
	}, worker.WithName("park"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "park", 0, client.WithID("term-chain-1"))
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
		if head.Status == client.StatusContinued {
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
				if tail, err := c.Get(ctx, tailID); err == nil && tail.Status == client.StatusRunning {
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
	} else if tail.Status != client.StatusTerminated {
		t.Fatalf("tail status=%q, want terminated", tail.Status)
	}
}

// TestClient_CancelFollowsContinueAsNew verifies Cancel reaches the live
// tail: a parked tail observes cancellation instead of hanging.
func TestClient_CancelFollowsContinueAsNew(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		if n == 0 {
			return 0, workflow.ContinueAsNew(wctx, 1)
		}
		if _, err := workflow.ReceiveSignal[string](wctx, "go"); err != nil {
			return 0, err
		}
		return 1, nil
	}, worker.WithName("park"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "park", 0, client.WithID("cancel-chain-1"))
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
		if head.Status == client.StatusContinued {
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
	if _, err := client.Result[int](rctx, h); err == nil {
		t.Fatal("expected cancellation error")
	}
}
