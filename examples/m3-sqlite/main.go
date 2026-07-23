package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func Hello(ctx *workflow.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func main() {
	dir := os.TempDir()
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	path := filepath.Join(dir, "tasuki-m3.db")
	ctx := context.Background()
	b, err := sqlite.New(path)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	if err := b.Migrate(ctx); err != nil {
		panic(err)
	}

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: 20 * time.Millisecond})
	tasuki.RegisterWorkflow(w, Hello, tasuki.WithName("hello"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	_ = c.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly-hello", Cron: "0 * * * *", Workflow: "hello",
		Input: []byte(`"from-cron"`),
	})

	h, err := tasuki.Start(ctx, c, "hello", "m3", tasuki.WithID("hello-m3"))
	if err != nil {
		panic(err)
	}
	res, err := tasuki.Result[string](ctx, h)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)

	list, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusCompleted})
	if err != nil {
		panic(err)
	}
	fmt.Printf("completed instances: %d\n", len(list))
}
