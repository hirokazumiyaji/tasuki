package tasuki_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

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
