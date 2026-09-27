//go:build tasuki_all

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
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

	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: 20 * time.Millisecond})
	worker.RegisterWorkflow(w, Hello, worker.WithName("hello"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	_ = c.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly-hello", Cron: "0 * * * *", Workflow: "hello",
		Input: []byte(`"from-cron"`),
	})

	h, err := client.Start(ctx, c, "hello", "m3", client.WithID("hello-m3"))
	if err != nil {
		panic(err)
	}
	res, err := client.Result[string](ctx, h)
	if err != nil {
		panic(err)
	}
	fmt.Println(res)

	list, err := c.List(ctx, client.InstanceFilter{Status: client.StatusCompleted})
	if err != nil {
		panic(err)
	}
	fmt.Printf("completed instances: %d\n", len(list))
}
