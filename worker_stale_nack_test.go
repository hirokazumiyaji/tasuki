package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// nackCountingBackend counts NackTask calls while delegating to the wrapped
// store, so tests can assert a stale worker issued no nack.
type nackCountingBackend struct {
	backend.Backend
	nacks atomic.Int32
}

func (b *nackCountingBackend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	b.nacks.Add(1)
	return b.Backend.NackTask(ctx, t, delay)
}

func (b *nackCountingBackend) nackCount() int {
	return int(b.nacks.Load())
}

// TestRequeueWorkflowTask_StaleNackSkippedAfterLeaseExpiry covers the
// reclaim race on the delayed-nack path: a worker whose handler outlives its
// lease must not nack after a peer reclaimed the task. NackTask updates by
// task ID only, so a stale nack would clear the peer's fresh lease and
// replace it with now+IncompatibleRetryDelay, letting a third worker claim
// while the peer executes. The stale worker must skip the nack (0 NackTask
// calls) and leave the peer lease intact; expiry reclaims naturally.
func TestRequeueWorkflowTask_StaleNackSkippedAfterLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &nackCountingBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		Queues:        []string{"default"},
		LeaseDuration: 150 * time.Millisecond,
	})

	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: "stale-nack-1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 150 * time.Millisecond, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(claimed))
	}
	stale := claimed[0]

	// Simulate a handler that outlives its lease: the local claim record
	// predates LeaseDuration, and the store lease has expired and been
	// reclaimed by a peer with a fresh token.
	w.trackWfClaim(stale.ID)
	w.wfClaimMu.Lock()
	w.wfClaim[stale.ID] = time.Now().Add(-time.Second)
	w.wfClaimMu.Unlock()
	mem.SetNow(t0.Add(10 * time.Second))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer reclaim: %v n=%d", err, len(peer))
	}
	if peer[0].WorkerID != "w2" {
		t.Fatalf("peer WorkerID=%q, want w2", peer[0].WorkerID)
	}

	w.requeueWorkflowTask(ctx, stale, errors.New("injected store failure"))

	if n := store.nackCount(); n != 0 {
		t.Fatalf("NackTask calls=%d, want 0 (stale worker must not nack a peer's fresh lease)", n)
	}
	// The peer lease (1m) must still be intact: advancing past the nack
	// delay (5s default) must not make the task claimable. A stale nack
	// would have replaced the lease with now+delay, so this probe would
	// find the task.
	mem.SetNow(t0.Add(10*time.Second + w.opts.IncompatibleRetryDelay + time.Second))
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 0 {
		t.Fatalf("probe claimed %d tasks, want 0 (peer lease was cleared by a stale nack)", len(probe))
	}
}

// TestRequeueWorkflowTask_FreshNackStillBacksOff guards the other side: a
// worker still within its lease must nack with a delay on non-contention
// failures (no behavior change for the common path).
func TestRequeueWorkflowTask_FreshNackStillBacksOff(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(now)
	store := &nackCountingBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{Queues: []string{"default"}})

	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: "fresh-nack-1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 30 * time.Second, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(claimed))
	}
	w.trackWfClaim(claimed[0].ID) // fresh claim within the lease
	w.requeueWorkflowTask(ctx, claimed[0], errors.New("injected store failure"))
	if n := store.nackCount(); n != 1 {
		t.Fatalf("NackTask calls=%d, want 1 (fresh worker must nack with a delay)", n)
	}
}
