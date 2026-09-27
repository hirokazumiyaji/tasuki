package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestSQLite_StaleRenewalVsPeerReclaimedLease is a regression test for
// the fenced-renewal finding, mirroring the memory-backend case: a
// renewal racing a peer reclaim after the lease lapses must not
// overwrite the successor's visible_at. The stale renewal affects zero
// rows and reports ErrNotFound while the current generation's renewal
// still extends the lease.
func TestSQLite_StaleRenewalVsPeerReclaimedLease(t *testing.T) {
	ctx := context.Background()
	b := openMigrated(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: 100 * time.Millisecond, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %#v", err, stale)
	}

	// Let the short lease lapse so a peer reclaims the task (new
	// generation: same id, new worker + bumped attempt).
	time.Sleep(200 * time.Millisecond)
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}

	// The stale holder's late renewal must affect zero rows and report
	// ErrNotFound: the lease moved on.
	if err := b.ExtendLease(ctx, stale[0], time.Second); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale ExtendLease = %v, want ErrNotFound (lease moved on)", err)
	}

	// The current generation's renewal still works.
	if err := b.ExtendLease(ctx, peer[0], time.Minute); err != nil {
		t.Fatalf("peer ExtendLease = %v, want nil", err)
	}
}
