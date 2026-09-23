package tasuki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestCommitWorkflow_StaleReleaseKeepsPeerLease covers the reclaim race on
// the commit-failure path: a slow worker that lost its lease to a peer must
// not clear the peer's fresh lease when its commit conflicts.
func TestCommitWorkflow_StaleReleaseKeepsPeerLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "sr1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	st, err := w.loadWorkflowState(ctx, "sr1")
	if err != nil {
		t.Fatal(err)
	}
	w1, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(w1) != 1 {
		t.Fatalf("claim w1: %v", err)
	}
	// w1's lease expires; a peer reclaims the same task.
	b.SetNow(now.Add(2 * time.Second))
	w2, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil || len(w2) != 1 {
		t.Fatalf("claim w2: %v", err)
	}
	// w1's slow commit conflicts; its fenced release must not clear w2.
	err = w.commitWorkflow(ctx, w1[0], st.Journal, backend.Advancement{
		InstanceID:  "sr1",
		TaskID:      w1[0].ID,
		ExpectedSeq: st.NextSeq - 1, // stale → ErrConflict
		NewEvents:   []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("got %v", err)
	}
	third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("stale commit release cleared peer lease: w3 claimed %+v", third)
	}
	// Sanity: the peer still owns the task and can release it.
	if err := b.ReleaseLease(ctx, w2[0]); err != nil {
		t.Fatalf("peer release: %v", err)
	}
}
