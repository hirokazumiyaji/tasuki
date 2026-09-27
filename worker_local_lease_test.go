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

// TestWorkflow_LocalActivityLeaseExtended verifies a local activity running
// longer than LeaseDuration does not lose its workflow task lease to a peer.
// Two workers race on the same backend while virtual time advances at wall
// speed; without periodic ExtendLease the second worker would reclaim the
// task and run the local activity a second time.
func TestWorkflow_LocalActivityLeaseExtended(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	var calls atomic.Int32
	slowLocal := func(actCtx context.Context, n int) (int, error) {
		calls.Add(1)
		select {
		case <-actCtx.Done():
			return 0, actCtx.Err()
		case <-time.After(600 * time.Millisecond):
			return n * 2, nil
		}
	}
	localWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "slowLocal", n)
	}

	mkWorker := func(id string) *tasuki.Worker {
		w := tasuki.NewWorker(b, tasuki.WorkerOptions{
			PollInterval:  5 * time.Millisecond,
			LeaseDuration: 150 * time.Millisecond,
			WorkerID:      id,
		})
		tasuki.RegisterActivity(w, slowLocal, tasuki.WithName("slowLocal"))
		tasuki.RegisterWorkflow(w, localWF, tasuki.WithName("localLeaseWF"))
		return w
	}
	w1 := mkWorker("w1")
	w2 := mkWorker("w2")
	w1.Start(ctx)
	w2.Start(ctx)
	defer w1.Shutdown(ctx)
	defer w2.Shutdown(ctx)

	// Advance the memory backend's virtual clock at wall speed so leases can
	// expire while the 600ms local activity runs.
	stopAdv := make(chan struct{})
	defer close(stopAdv)
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopAdv:
				return
			case <-tk.C:
				b.SetNow(b.Now().Add(10 * time.Millisecond))
			}
		}
	}()

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "localLeaseWF", 21, tasuki.WithID("local-lease-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var got int
	for {
		info, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info != nil && info.Status == "completed" {
			got, err = tasuki.Result[int](ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if info != nil && (info.Status == "failed" || info.Status == "stuck") {
			t.Fatalf("status=%s failure=%s", info.Status, string(info.Failure))
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for completion, calls=%d", calls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got != 42 {
		t.Fatalf("got %d want 42", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("local activity executed %d times, want exactly once (lease not extended?)", n)
	}
}

// TestWorkflow_LocalActivityShutdownCancel verifies a running local activity
// observes worker shutdown via context cancellation.
func TestWorkflow_LocalActivityShutdownCancel(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	started := make(chan struct{}, 1)
	var cancelled atomic.Bool
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-actCtx.Done():
			cancelled.Store(true)
			return "", actCtx.Err()
		case <-time.After(10 * time.Second):
			return "slow", nil
		}
	}, tasuki.WithName("blockLocal"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "blockLocal", struct{}{})
	}, tasuki.WithName("cancelWF"))

	w.Start(ctx)
	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "cancelWF", struct{}{}, tasuki.WithID("local-cancel-1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("local activity did not start")
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := w.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("Shutdown took %v; local activity ignored cancellation", elapsed)
	}
	if !cancelled.Load() {
		t.Fatal("local activity did not observe shutdown cancellation")
	}
}

// TestWorkflow_LocalActivityTimeout verifies WorkerOptions.LocalActivityTimeout
// bounds a single ExecuteLocal invocation.
func TestWorkflow_LocalActivityTimeout(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         time.Millisecond,
		LeaseDuration:        time.Minute,
		LocalActivityTimeout: 50 * time.Millisecond,
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-actCtx.Done():
			return "", actCtx.Err()
		case <-time.After(5 * time.Second):
			return "slow", nil
		}
	}, tasuki.WithName("timeoutLocal"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "timeoutLocal", struct{}{})
	}, tasuki.WithName("timeoutWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "timeoutWF", struct{}{}, tasuki.WithID("local-timeout-1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = tasuki.Result[string](ctx, h)
	if err == nil {
		t.Fatal("expected timeout failure")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
}
