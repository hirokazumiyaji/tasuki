package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func OrderWorkflow(ctx *workflow.Context, orderID string) (string, error) {
	invoice, err := workflow.Execute[string, string](ctx, "charge", orderID)
	if err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	return invoice, nil
}

func Charge(ctx context.Context, orderID string) (string, error) {
	return "inv-" + orderID, nil
}

func main() {
	dsn := os.Getenv("TASUKI_SPANNER_DSN")
	if dsn == "" {
		dsn = "projects/tasuki/instances/tasuki/databases/tasuki"
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		_ = os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	}
	ctx := context.Background()
	if err := spanner.RecreateDatabase(ctx, dsn); err != nil {
		panic(err)
	}
	b, err := spanner.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	if err := b.Migrate(ctx); err != nil {
		panic(err)
	}

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: 50 * time.Millisecond})
	tasuki.RegisterWorkflow(w, OrderWorkflow, tasuki.WithName("order"))
	tasuki.RegisterActivity(w, Charge, tasuki.WithName("charge"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "order", "123", tasuki.WithID("order-spanner-123"))
	if err != nil && !errors.Is(err, tasuki.ErrAlreadyStarted) {
		panic(err)
	}
	res, err := tasuki.Result[string](ctx, h)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}
