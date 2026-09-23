package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestMemory_StaleReleaseVsPeerReclaimedLease is a regression test for the
// fenced-release finding: a renewal delayed past the lease lets a peer
// reclaim the task, and the stale holder's shutdown release must not clear
// the peer's fresh lease (or a third worker executes concurrently with the
// peer). The stale release reports ErrNotFound with zero effect while the
// current generation's release still works.
func TestMemory_StaleReleaseVsPeerReclaimedLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(t0)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %#v", err, stale)
	}

	// The lease lapses and a peer reclaims the task (new generation:
	// same id, new worker + bumped attempt).
	b.SetNow(t0.Add(2 * time.Minute))
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}

	// The stale holder's late release must be a no-op reporting
	// ErrNotFound: the lease moved on.
	if err := b.ReleaseLease(ctx, stale[0]); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale ReleaseLease = %v, want ErrNotFound (lease moved on)", err)
	}
	// The peer's lease is intact: a third worker finds nothing claimable.
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third worker claimed %d tasks, want 0 (peer lease must stay intact)", len(third))
	}
	// The current generation's release still works and frees the task.
	if err := b.ReleaseLease(ctx, peer[0]); err != nil {
		t.Fatalf("peer ReleaseLease = %v, want nil", err)
	}
	freed, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(freed) != 1 {
		t.Fatalf("re-claim after peer release = %d tasks, want 1", len(freed))
	}
}

// TestMemory_ReleaseLeaseFenced covers the reclaim race: after w1's lease
// expires and w2 reclaims the task, w1's stale release must not clear w2's
// fresh lease.
func TestMemory_ReleaseLeaseFenced(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "f1", Name: "WF", Queue: "default"}); err != nil {
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
	// Stale release must be a no-op (ErrNotFound), not clear w2's lease.
	if err := b.ReleaseLease(ctx, stale); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale release: want ErrNotFound, got %v", err)
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
		t.Fatalf("stale release cleared peer lease: w3 claimed %+v", third)
	}
	// The current owner can still release.
	if err := b.ReleaseLease(ctx, w2[0]); err != nil {
		t.Fatalf("owner release: %v", err)
	}
	fourth, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w3",
	})
	if err != nil || len(fourth) != 1 {
		t.Fatalf("reclaim after owner release: %v %#v", err, fourth)
	}
}
