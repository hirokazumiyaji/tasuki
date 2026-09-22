package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func HelloWorkflow(ctx *workflow.Context, name string) (string, error) {
	msg, err := workflow.Execute[string, string](ctx, "greet", name)
	if err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, time.Hour); err != nil {
		return "", err
	}
	return msg + " (after sleep)", nil
}

func Greet(ctx context.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func main() {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: 10 * time.Millisecond})
	tasuki.RegisterWorkflow(w, HelloWorkflow, tasuki.WithName("hello"))
	tasuki.RegisterActivity(w, Greet, tasuki.WithName("greet"))
	if err := w.StartWithError(ctx); err != nil {
		panic(err)
	}
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	_, err := tasuki.Start(ctx, c, "hello", "world", tasuki.WithID("demo-1"))
	if err != nil {
		panic(err)
	}

	for {
		info, err := c.Get(ctx, "demo-1")
		if err != nil {
			panic(err)
		}
		if info.Status == "completed" {
			fmt.Println(string(info.Result))
			return
		}
		if info.Status == "failed" || info.Status == "stuck" {
			panic(string(info.Failure))
		}
		if !b.HasRunnableTasks() {
			if next, ok := b.NextTimerFireAt(); ok && next.After(b.Now()) {
				b.SetNow(next)
				continue
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}
