package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestSQLite_ReleaseLeaseFenced covers the reclaim race: after w1's lease
// expires and w2 reclaims the task, w1's stale release must not clear w2's
// fresh lease.
func TestSQLite_ReleaseLeaseFenced(t *testing.T) {
	ctx := context.Background()
	b := openMigrated(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "f1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w1, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 20 * time.Millisecond, WorkerID: "w1",
	})
	if err != nil || len(w1) != 1 {
		t.Fatalf("claim w1: %v %#v", err, w1)
	}
	stale := w1[0]
	time.Sleep(50 * time.Millisecond)
	w2, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil || len(w2) != 1 {
		t.Fatalf("claim w2: %v %#v", err, w2)
	}
	if err := b.ReleaseLease(ctx, stale); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale release: want ErrNotFound, got %v", err)
	}
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("stale release cleared peer lease: w3 claimed %+v", third)
	}
	if err := b.ReleaseLease(ctx, w2[0]); err != nil {
		t.Fatalf("owner release: %v", err)
	}
}
