package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_NackUnregisteredWorkflow(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("inc-1")); err != nil {
		t.Fatal(err)
	}

	old := worker.NewWorker(b, worker.WorkerOptions{
		PollInterval:           time.Millisecond,
		IncompatibleRetryDelay: time.Hour,
	})
	old.Start(ctx)
	defer old.Shutdown(ctx)
	time.Sleep(30 * time.Millisecond)

	info, _ := c.Get(ctx, "inc-1")
	if info == nil || info.Status != "running" {
		t.Fatalf("status=%v", info)
	}
	if info.Status == "stuck" {
		t.Fatal("old worker must not mark stuck")
	}
	if err := old.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	b.SetNow(b.Now().Add(2 * time.Hour))
	newW := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(newW, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("WF"))
	newW.Start(ctx)
	defer newW.Shutdown(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, "inc-1")
		if info != nil && info.Status == "completed" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ = c.Get(ctx, "inc-1")
	t.Fatalf("timeout status=%v", info)
}

func TestWorker_NackDeterminismInsteadOfStuck(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	seed := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(seed, func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("Charge"))
	worker.RegisterWorkflow(seed, func(wctx *workflow.Context, _ struct{}) (string, error) {
		if _, err := workflow.Execute[struct{}, string](wctx, "Charge", struct{}{}); err != nil {
			return "", err
		}
		return "", workflow.Sleep(wctx, time.Hour)
	}, worker.WithName("WF"))
	seed.Start(ctx)

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("inc-det")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "inc-det")
		if st != nil && len(st.Journal) >= 3 { // started, scheduled, completed (+ maybe timer)
			hasTimer := false
			for _, ev := range st.Journal {
				if ev.Type == journal.TypeTimerCreated {
					hasTimer = true
				}
			}
			if hasTimer {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = seed.Shutdown(ctx)

	info, _ := c.Get(ctx, "inc-det")
	if info == nil || info.Status != "running" {
		t.Fatalf("want running after charge+sleep, got %#v", info)
	}

	if next, ok := b.NextTimerFireAt(); ok {
		b.SetNow(next)
	}

	old := worker.NewWorker(b, worker.WorkerOptions{
		PollInterval:           time.Millisecond,
		IncompatibleRetryDelay: time.Hour,
	})
	worker.RegisterActivity(old, func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("Refund"))
	worker.RegisterWorkflow(old, func(wctx *workflow.Context, _ struct{}) (string, error) {
		if _, err := workflow.Execute[struct{}, string](wctx, "Refund", struct{}{}); err != nil {
			return "", err
		}
		return "", workflow.Sleep(wctx, time.Hour)
	}, worker.WithName("WF"))
	old.Start(ctx)
	defer old.Shutdown(ctx)

	for time.Now().Before(deadline) {
		info, _ := c.Get(ctx, "inc-det")
		if info != nil && info.Status == "stuck" {
			t.Fatal("determinism must nack, not stuck")
		}
		time.Sleep(30 * time.Millisecond)
		info, _ = c.Get(ctx, "inc-det")
		if info != nil && info.Status == "running" {
			return
		}
	}
	t.Fatal("timeout")
}

func TestWorker_NackUnregisteredActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := worker.NewWorker(b, worker.WorkerOptions{
		PollInterval:           time.Millisecond,
		IncompatibleRetryDelay: time.Hour,
	})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "Missing", struct{}{})
	}, worker.WithName("WF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("inc-act")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.LoadWorkflow(ctx, "inc-act")
		if st == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		scheduled := false
		for _, ev := range st.Journal {
			if ev.Type == journal.TypeActivityFailed {
				t.Fatal("unregistered activity must not fail the activity")
			}
			if ev.Type == journal.TypeActivityScheduled {
				scheduled = true
			}
		}
		info, _ := c.Get(ctx, "inc-act")
		if info != nil && info.Status == "failed" {
			t.Fatal("workflow must not fail from unregistered activity nack")
		}
		if scheduled && info != nil && info.Status == "running" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
