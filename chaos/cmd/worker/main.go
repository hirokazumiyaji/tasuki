//go:build tasuki_all

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	b, closer := openBackend(ctx)
	defer closer()

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

func openBackend(ctx context.Context) (backend.Backend, func()) {
	switch os.Getenv("TASUKI_BACKEND") {
	case "mysql":
		dsn := os.Getenv("TASUKI_MYSQL_DSN")
		if dsn == "" {
			panic("TASUKI_MYSQL_DSN required")
		}
		b, err := mysql.New(ctx, dsn)
		if err != nil {
			panic(err)
		}
		return b, func() { _ = b.Close() }
	case "spanner":
		dsn := os.Getenv("TASUKI_SPANNER_DSN")
		if dsn == "" {
			panic("TASUKI_SPANNER_DSN required")
		}
		b, err := spanner.New(ctx, dsn)
		if err != nil {
			panic(err)
		}
		return b, func() { _ = b.Close() }
	case "dynamodb":
		ep := os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
		b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: ep})
		if err != nil {
			panic(err)
		}
		return b, func() { _ = b.Close() }
	case "firestore":
		b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
		if err != nil {
			panic(err)
		}
		return b, func() { _ = b.Close() }
	case "sqlite":
		path := os.Getenv("TASUKI_SQLITE_PATH")
		if path == "" {
			panic("TASUKI_SQLITE_PATH required")
		}
		b, err := sqlite.New(path)
		if err != nil {
			panic(err)
		}
		return b, func() { _ = b.Close() }
	default:
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			panic("TASUKI_POSTGRES_DSN required")
		}
		b, err := postgres.New(ctx, dsn)
		if err != nil {
			panic(err)
		}
		return b, b.Close
	}
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
	return n + 1, nil
}
