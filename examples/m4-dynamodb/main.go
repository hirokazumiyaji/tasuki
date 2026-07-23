package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
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
	ep := os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	if ep == "" {
		ep = "http://localhost:8000"
	}
	_ = os.Setenv("AWS_ACCESS_KEY_ID", envOr("AWS_ACCESS_KEY_ID", "local"))
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", envOr("AWS_SECRET_ACCESS_KEY", "local"))
	_ = os.Setenv("AWS_REGION", envOr("AWS_REGION", "us-east-1"))

	ctx := context.Background()
	b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: ep})
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
	h, err := tasuki.Start(ctx, c, "order", "123", tasuki.WithID("order-ddb-123"))
	if err != nil && !errors.Is(err, tasuki.ErrAlreadyStarted) {
		panic(err)
	}
	res, err := tasuki.Result[string](ctx, h)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
