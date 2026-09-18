package tasuki_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// goroutineID returns the current goroutine's ID for test assertions.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id int64
	_, _ = fmt.Sscanf(string(buf[:n]), "goroutine %d ", &id)
	return id
}

func TestUpdate_SyncHandler(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.SetUpdateHandler(wctx, "bump", func(wctx *workflow.Context, n int) (int, error) {
			workflow.UpsertMemo(wctx, map[string]string{"n": fmt.Sprintf("%d", n)})
			return n + 1, nil
		})
		return "", workflow.Sleep(wctx, time.Hour)
	}, tasuki.WithName("updWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "updWF", struct{}{}, tasuki.WithID("upd-1")); err != nil {
		t.Fatal(err)
	}
	// Wait until sleeping (timer recorded).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "upd-1")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got, err := tasuki.Update[int, int](ctx, w, "upd-1", "bump", 41, tasuki.WithUpdateID("u-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d", got)
	}

	// Idempotent replay.
	got2, err := tasuki.Update[int, int](ctx, w, "upd-1", "bump", 99, tasuki.WithUpdateID("u-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got2 != 42 {
		t.Fatalf("idempotent got %d", got2)
	}

	inst, err := c.Get(ctx, "upd-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["n"] != "41" {
		t.Fatalf("memo=%v", inst.Memo)
	}
}

func TestUpdate_WithActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}, tasuki.WithName("double"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.SetUpdateHandler(wctx, "work", func(wctx *workflow.Context, n int) (int, error) {
			return workflow.Execute[int, int](wctx, "double", n)
		})
		return "", workflow.Sleep(wctx, time.Hour)
	}, tasuki.WithName("updActWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "updActWF", struct{}{}, tasuki.WithID("upd-act-1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "upd-act-1")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got, err := tasuki.Update[int, int](ctx, w, "upd-act-1", "work", 21, tasuki.WithUpdateID("ua-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d", got)
	}
}

func TestUpdate_UnknownHandler(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "", workflow.Sleep(wctx, time.Hour)
	}, tasuki.WithName("updMissWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "updMissWF", struct{}{}, tasuki.WithID("upd-miss")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "upd-miss")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, err := tasuki.Update[int, int](ctx, w, "upd-miss", "nope", 1, tasuki.WithUpdateID("um-1"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, workflow.ErrUnknownUpdate) && err.Error() != workflow.ErrUnknownUpdate.Error() {
		// FormatUpdateError wraps as plain string matching ErrUnknownUpdate.Error()
		if err.Error() != workflow.ErrUnknownUpdate.Error() {
			t.Fatalf("err=%v", err)
		}
	}
}

// TestUpdate_DoesNotExecuteActivitiesOnCallerGoroutine verifies that Update
// never drives worker ticks itself: with no started Worker loop, Update must
// not execute activity functions on the caller goroutine (it only enqueues
// the request and waits for completion).
func TestUpdate_DoesNotExecuteActivitiesOnCallerGoroutine(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})

	var activityRan atomic.Bool
	var activityGID atomic.Int64
	tasuki.RegisterActivity(w, func(_ context.Context, n int) (int, error) {
		activityRan.Store(true)
		activityGID.Store(goroutineID())
		return n * 2, nil
	}, tasuki.WithName("isoDouble"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.SetUpdateHandler(wctx, "work", func(wctx *workflow.Context, n int) (int, error) {
			return workflow.Execute[int, int](wctx, "isoDouble", n)
		})
		return "", workflow.Sleep(wctx, time.Hour)
	}, tasuki.WithName("updIsoWF"))

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "updIsoWF", struct{}{}, tasuki.WithID("upd-iso-1")); err != nil {
		t.Fatal(err)
	}
	// Drive the instance to sleeping with explicit PollOnce calls only
	// (no background worker loop running).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w.PollOnce(ctx)
		st, _ := b.LoadWorkflow(ctx, "upd-iso-1")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if activityRan.Load() {
		t.Fatal("activity ran during setup")
	}

	callerGID := goroutineID()

	// No Worker loop is running, so Update must not make progress by itself:
	// it must time out without executing the update's activity on this goroutine.
	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := tasuki.Update[int, int](waitCtx, w, "upd-iso-1", "work", 21, tasuki.WithUpdateID("iso-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded without a running worker, got %v", err)
	}
	if activityRan.Load() {
		t.Fatalf("activity executed on Update caller goroutine (activity gid=%d, caller gid=%d)",
			activityGID.Load(), callerGID)
	}

	// With a started Worker loop driving progress, the same update completes
	// and the activity runs off the caller goroutine.
	w.Start(ctx)
	defer w.Shutdown(ctx)
	got, err := tasuki.Update[int, int](ctx, w, "upd-iso-1", "work", 21, tasuki.WithUpdateID("iso-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d", got)
	}
	if !activityRan.Load() {
		t.Fatal("activity did not run after worker start")
	}
	if activityGID.Load() == callerGID {
		t.Fatalf("activity ran on Update caller goroutine (gid=%d)", callerGID)
	}
}
