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
