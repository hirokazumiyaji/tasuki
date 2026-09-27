package tasuki_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_QuerySuspendedState(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		n := 0
		workflow.SetQueryHandler(wctx, "count", func(_ struct{}) (int, error) { return n, nil })
		n = 1
		if err := workflow.Sleep(wctx, time.Hour); err != nil {
			return "", err
		}
		n = 2
		return "done", nil
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("q-1")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "q-1")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	before, err := b.LoadWorkflow(ctx, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := tasuki.Query[struct{}, int](ctx, w, "q-1", "count", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("count=%d want 1", got)
	}

	after, err := b.LoadWorkflow(ctx, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	if after.NextSeq != before.NextSeq {
		t.Fatalf("next_seq changed: %d -> %d", before.NextSeq, after.NextSeq)
	}
	if len(after.Journal) != len(before.Journal) {
		t.Fatalf("journal grew: %d -> %d", len(before.Journal), len(after.Journal))
	}
}

func TestWorker_QueryAfterProgress(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		n := 0
		workflow.SetQueryHandler(wctx, "count", func(_ struct{}) (int, error) { return n, nil })
		n = 1
		if err := workflow.Sleep(wctx, time.Hour); err != nil {
			return "", err
		}
		n = 2
		if err := workflow.Sleep(wctx, time.Hour); err != nil {
			return "", err
		}
		return "done", nil
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("q-2")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "q-2")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got, err := tasuki.Query[struct{}, int](ctx, w, "q-2", "count", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("before fire count=%d want 1", got)
	}

	if next, ok := b.NextTimerFireAt(); ok {
		b.SetNow(next)
	}
	for time.Now().Before(deadline.Add(2 * time.Second)) {
		st, _ := b.LoadWorkflow(ctx, "q-2")
		if st != nil && len(st.Journal) >= 4 { // started, timer, fired, timer2
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	got, err = tasuki.Query[struct{}, int](ctx, w, "q-2", "count", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("after fire count=%d want 2", got)
	}
}

func TestWorker_QueryUnknownName(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.SetQueryHandler(wctx, "count", func(_ struct{}) (int, error) { return 0, nil })
		return "", workflow.Sleep(wctx, time.Hour)
	}, tasuki.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("q-3")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "q-3")
		if st != nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, err := tasuki.Query[struct{}, int](ctx, w, "q-3", "missing", struct{}{})
	if !errors.Is(err, workflow.ErrUnknownQuery) {
		t.Fatalf("got %v", err)
	}
}

func TestWorker_QueryNotFound(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, tasuki.WithName("WF"))

	_, err := tasuki.Query[struct{}, int](ctx, w, "nope", "count", struct{}{})
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestWorker_QueryDoesNotRunUnrecordedLocalActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{})
	activityCalls := 0
	tasuki.RegisterActivity(w, func(context.Context, struct{}) (struct{}, error) {
		activityCalls++
		return struct{}{}, nil
	}, tasuki.WithName("sideEffect"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (struct{}, error) {
		workflow.SetQueryHandler(wctx, "calls", func(struct{}) (int, error) {
			return activityCalls, nil
		})
		_, err := workflow.ExecuteLocal[struct{}, struct{}](wctx, "sideEffect", struct{}{})
		return struct{}{}, err
	}, tasuki.WithName("queryLocalActivity"))

	client := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, client, "queryLocalActivity", struct{}{}, tasuki.WithID("query-local-activity")); err != nil {
		t.Fatal(err)
	}

	calls, err := tasuki.Query[struct{}, int](ctx, w, "query-local-activity", "calls", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || activityCalls != 0 {
		t.Fatalf("query ran local activity: query calls=%d activity calls=%d", calls, activityCalls)
	}
}
