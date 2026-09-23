package spanner_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestCommitAdvancementEnsureWorkflowTaskFollowUp is a regression test for the
// Spanner read-your-writes race in enqueueWorkflowTask: Spanner buffers
// mutations client-side until commit, so the existence check must ignore the
// owned workflow task deleted in the same commit. Without that, the check
// sees the stale pre-commit row and skips the follow-up insert, leaving no
// claimable workflow task (conformance TerminalCleanup failed with
// "claim wf for terminal" empty).
func TestCommitAdvancementEnsureWorkflowTaskFollowUp(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	if err := spanner.RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := spanner.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const id = "ensure-followup-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim wf: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	actSeq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  id,
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: actSeq, Type: journal.TypeActivityScheduled, Name: "work"},
		},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id, Name: "work", Seq: actSeq, Input: []byte(`{}`),
		}},
		EnsureWorkflowTask: true,
	}); err != nil {
		t.Fatal(err)
	}
	follow, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(follow) != 1 || follow[0].InstanceID != id {
		t.Fatalf("want 1 follow-up workflow task for %s, got %#v", id, follow)
	}
}
