package tasuki_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_JournalWarnThreshold(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         time.Millisecond,
		JournalWarnThreshold: 3,
		Logger:               log,
	})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
		for i := 0; i < 5; i++ {
			if _, err := workflow.SideEffect(wctx, func() int { return i }); err != nil {
				return 0, err
			}
		}
		if err := workflow.Sleep(wctx, time.Millisecond); err != nil {
			return 0, err
		}
		return 1, nil
	}, tasuki.WithName("fat"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "fat", struct{}{}, tasuki.WithID("fat-1")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "journal size warning") {
			return
		}
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) && !b.HasRunnableTasks() {
			b.SetNow(next)
		}
		info, err := c.Get(ctx, "fat-1")
		if err == nil && info.Status == "completed" {
			t.Fatalf("completed without warn; log=%s", buf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for journal warn; log=%s", buf.String())
}

func TestWorker_JournalWarnThreshold_Disabled(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         time.Millisecond,
		JournalWarnThreshold: -1,
		Logger:               log,
	})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
		for i := 0; i < 5; i++ {
			if _, err := workflow.SideEffect(wctx, func() int { return i }); err != nil {
				return 0, err
			}
		}
		if err := workflow.Sleep(wctx, time.Millisecond); err != nil {
			return 0, err
		}
		return 1, nil
	}, tasuki.WithName("fat-off"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "fat-off", struct{}{}, tasuki.WithID("fat-off-1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) && !b.HasRunnableTasks() {
			b.SetNow(next)
		}
		info, err := c.Get(ctx, "fat-off-1")
		if err == nil && info.Status == "completed" {
			if strings.Contains(buf.String(), "journal size warning") {
				t.Fatalf("unexpected warn when disabled; log=%s", buf.String())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for completion")
}
