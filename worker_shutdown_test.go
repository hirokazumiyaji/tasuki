package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ShutdownReleasesLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)

	started := make(chan struct{})
	block := make(chan struct{})
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		close(started)
		<-block
		return "x", nil
	}, tasuki.WithName("slow"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "slow", struct{}{})
	}, tasuki.WithName("WF"))

	c := tasuki.NewClient(b)
	_, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shut-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("activity did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_ = w.Shutdown(shutdownCtx) // timeout while activity blocked

	// Lease released while the original activity goroutine is still blocked.
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	close(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected released activity claimable, got %d", len(tasks))
	}
}
