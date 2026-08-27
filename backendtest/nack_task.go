package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

func testNackTask(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("nack-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(tasks))
	}
	at := time.Now().UTC().Add(time.Hour)
	if cs, ok := b.(ClockSetter); ok {
		at = cs.Now().Add(time.Hour)
	}
	if err := b.NackTask(ctx, tasks[0], at); err != nil {
		t.Fatal(err)
	}
	// Immediate reclaim should fail while visible_at is in the future.
	again, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("claimed %d want 0 while nacked into the future", len(again))
	}
}
