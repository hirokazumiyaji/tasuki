package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestSQLite_StaleCommitVsPeerReclaimedLease is a regression test for the
// round-8 commit-fencing finding: a slow CommitAdvancement starting after
// the worker's in-memory preflight loses its lease to a Shutdown release +
// peer reclaim. The commit must be conditioned on the claimed generation
// (worker_id + attempt): the stale commit reports ErrConflict without
// deleting the peer's active task. Without the fence the ID-only predicate
// deletes the peer task and persists the stale advancement.
func TestSQLite_StaleCommitVsPeerReclaimedLease(t *testing.T) {
	ctx := context.Background()
	b := openMigrated(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: 200 * time.Millisecond, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %#v", err, stale)
	}
	st, err := b.LoadWorkflowHead(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	staleAdv := backend.Advancement{
		InstanceID:  "i1",
		TaskID:      stale[0].ID,
		ExpectedSeq: st.NextSeq,
		WorkerID:    stale[0].WorkerID,
		Attempt:     stale[0].Attempt,
		Terminal:    &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
		NewEvents:   nil,
	}
	if staleAdv.WorkerID == "" {
		t.Fatal("claimed task carries no generation")
	}

	// Lease lapses; a peer reclaims with a new generation.
	time.Sleep(300 * time.Millisecond)
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}
	if peer[0].ID != stale[0].ID {
		t.Fatalf("peer task id=%d, want reclaimed %d", peer[0].ID, stale[0].ID)
	}
	if peer[0].WorkerID == staleAdv.WorkerID && peer[0].Attempt == staleAdv.Attempt {
		t.Fatal("peer reclaim did not move the generation")
	}

	// Stale fenced commit must fail without touching the peer task.
	if err := b.CommitAdvancement(ctx, staleAdv); !errors.Is(err, backend.ErrConflict) && !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale CommitAdvancement = %v, want ErrConflict/ErrNotFound (fenced)", err)
	}
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third claimed %d, want 0 (peer lease intact)", len(third))
	}
	inst, err := b.GetInstance(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "running" {
		t.Fatalf("status=%s, want running (stale commit must not persist)", inst.Status)
	}

	// Fresh generation still commits.
	pst, err := b.LoadWorkflowHead(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	fresh := backend.Advancement{
		InstanceID:  "i1",
		TaskID:      peer[0].ID,
		ExpectedSeq: pst.NextSeq,
		WorkerID:    peer[0].WorkerID,
		Attempt:     peer[0].Attempt,
		Terminal:    &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}
	if err := b.CommitAdvancement(ctx, fresh); err != nil {
		t.Fatalf("fresh CommitAdvancement = %v, want nil", err)
	}
	inst, err = b.GetInstance(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "completed" {
		t.Fatalf("status=%s, want completed", inst.Status)
	}
}
