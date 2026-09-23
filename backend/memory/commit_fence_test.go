package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestMemory_StaleCommitVsPeerReclaimedLease mirrors the sqlite commit-fence
// regression: a stale fenced CommitAdvancement after a peer reclaim must
// report ErrConflict without deleting the peer's task.
func TestMemory_StaleCommitVsPeerReclaimedLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v", err)
	}
	st, err := b.LoadWorkflowHead(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	staleAdv := backend.Advancement{
		InstanceID: "i1", TaskID: stale[0].ID, ExpectedSeq: st.NextSeq,
		WorkerID: stale[0].WorkerID, Attempt: stale[0].Attempt,
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}
	// Simulate a Shutdown release + peer reclaim with a new generation: the
	// release is fenced but succeeds for the owner, then the peer claims.
	if err := b.ReleaseLease(ctx, stale[0]); err != nil {
		t.Fatalf("release: %v", err)
	}
	peer, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v", err)
	}
	if err := b.CommitAdvancement(ctx, staleAdv); !errors.Is(err, backend.ErrConflict) && !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stale commit = %v, want conflict", err)
	}
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third claimed %d, want 0", len(third))
	}
}
