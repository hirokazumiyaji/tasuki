package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func main() {
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		panic("TASUKI_POSTGRES_DSN required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	b, err := postgres.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer b.Close()

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  20 * time.Millisecond,
		LeaseDuration: time.Second,
		WorkerID:      os.Getenv("WORKER_ID"),
	})
	tasuki.RegisterActivity(w, step, tasuki.WithName("step"))
	tasuki.RegisterWorkflow(w, chaosWF, tasuki.WithName("chaos"))
	w.Start(ctx)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Shutdown(shutdownCtx)
}

func chaosWF(ctx *workflow.Context, n int) (int, error) {
	a, err := workflow.Execute[int, int](ctx, "step", n)
	if err != nil {
		return 0, err
	}
	b, err := workflow.Execute[int, int](ctx, "step", a)
	if err != nil {
		return 0, err
	}
	return b, nil
}

func step(ctx context.Context, n int) (int, error) {
	time.Sleep(5 * time.Millisecond)
	return n + 1, nil
}
