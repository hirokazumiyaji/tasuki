package worker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestLocalActivity_ExecuteAndNoActivityTask(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterActivity(w, func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}, worker.WithName("double"))
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "double", n)
	}, worker.WithName("localWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "localWF", 21, client.WithID("la-1"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Result[int](ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d", got)
	}

	st, err := b.LoadWorkflow(ctx, "la-1")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range st.Journal {
		if ev.Type == journal.TypeLocalActivity {
			found = true
			if ev.Name != "double" {
				t.Fatalf("name=%q", ev.Name)
			}
		}
		if ev.Type == journal.TypeActivityScheduled {
			t.Fatal("local activity must not schedule activity tasks")
		}
	}
	if !found {
		t.Fatal("missing local_activity journal event")
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10, Lease: time.Second, WorkerID: "check",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("unexpected activity tasks: %+v", tasks)
	}
}

func TestLocalActivity_UnregisteredFails(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "missing", struct{}{})
	}, worker.WithName("localMissingWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "localMissingWF", struct{}{}, client.WithID("la-missing"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Result[string](ctx, h)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "activity not registered") {
		t.Fatalf("err=%v", err)
	}

	st, err := b.LoadWorkflow(ctx, "la-missing")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range st.Journal {
		if ev.Type == journal.TypeLocalActivity {
			found = true
		}
	}
	if !found {
		t.Fatal("failure should still journal local_activity")
	}
}
