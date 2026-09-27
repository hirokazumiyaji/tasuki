package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestMemory_StaleRenewalVsPeerReclaimedLease is a regression test for
// the fenced-renewal finding: a renewal delayed past the lease lets a
// peer reclaim the task, and the stale holder's late ExtendLease must not
// overwrite the peer's post-reclaim visible_at (or the peer's retry stays
// hidden and a third worker executes concurrently with it). The stale
// renewal reports ErrNotFound with zero effect while the current
// generation's renewal still works.
func TestMemory_StaleRenewalVsPeerReclaimedLease(t *testing.T) {
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
	// same id, new worker + bumped attempt, visible for a minute).
	b.SetNow(t0.Add(2 * time.Minute))
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}

	// The stale holder's late renewal (short lease, so an applied
	// overwrite would be observable) must be a no-op reporting
	// ErrNotFound: the lease moved on.
	if err := b.ExtendLease(ctx, stale[0], time.Second); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale ExtendLease = %v, want ErrNotFound (lease moved on)", err)
	}
	// The peer's retry visibility is intact: past the stale lease but
	// inside the peer's minute, a third worker finds nothing claimable.
	// An applied stale renewal would have replaced the peer's minute
	// with one second and exposed the task here.
	b.SetNow(t0.Add(2*time.Minute + 30*time.Second))
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third worker claimed %d tasks, want 0 (stale renewal overwrote the peer's lease)", len(third))
	}

	// The current generation's renewal still works and extends the lease
	// past the peer's original visibility.
	if err := b.ExtendLease(ctx, peer[0], time.Minute); err != nil {
		t.Fatalf("peer ExtendLease = %v, want nil", err)
	}
	b.SetNow(t0.Add(3*time.Minute + 10*time.Second))
	fourth, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "fourth",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fourth) != 0 {
		t.Fatalf("fourth worker claimed %d tasks, want 0 (peer renewal must extend the lease)", len(fourth))
	}
}
