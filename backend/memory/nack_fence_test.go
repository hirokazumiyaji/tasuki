package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestMemory_NackTaskFenced covers the reclaim race on the delayed-nack
// path: after w1's lease expires and w2 reclaims the task, w1's stale nack
// must report ErrNotFound without touching w2's fresh lease — even when the
// worker's local-expiry precheck passed and the nack was actually issued
// (the check→NackTask gap is non-atomic).
func TestMemory_NackTaskFenced(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "n1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w1, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(w1) != 1 {
		t.Fatalf("claim w1: %v %#v", err, w1)
	}
	stale := w1[0]
	// Expire w1's lease and let w2 reclaim.
	b.SetNow(now.Add(2 * time.Second))
	w2, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil || len(w2) != 1 {
		t.Fatalf("claim w2: %v %#v", err, w2)
	}
	if w2[0].Attempt == stale.Attempt || w2[0].WorkerID == stale.WorkerID {
		t.Fatalf("want new claim token, got stale=%+v w2=%+v", stale, w2[0])
	}
	// Stale nack must be a no-op (ErrNotFound), not clear w2's lease.
	if err := b.NackTask(ctx, stale, time.Minute); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale nack: want ErrNotFound, got %v", err)
	}
	// w2's lease must still hold: no third worker can claim.
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("stale nack cleared peer lease: w3 claimed %+v", third)
	}
	// The current owner can still nack with a delay.
	if err := b.NackTask(ctx, w2[0], time.Minute); err != nil {
		t.Fatalf("owner nack: %v", err)
	}
	fourth, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fourth) != 0 {
		t.Fatalf("owner nack did not defer visibility: w3 claimed %+v", fourth)
	}
}
