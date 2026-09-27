package spanner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func commitTerminateTestBackend(t *testing.T) (*Backend, context.Context, string) {
	t.Helper()
	dsn := guardTestDSN(t)
	ctx := context.Background()
	if err := RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, dsn)
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
	return b, ctx, dsn
}

// flipStatusWithoutSweep commits the terminate status flip but skips the
// task sweep, deterministically modeling the race window between the status
// commit and the later sweep: the leased task row still exists, so only the
// in-transaction running-status gate can reject the commit.
func flipStatusWithoutSweep(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id":           id,
				"status":       "terminated",
				"updated_at":   nowUTC(),
				"completed_at": nowUTC(),
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A workflow commit racing termination must be rejected: the leased task
// still matches ExpectedSeq/TaskID inside the race window, so without the
// running-status gate a terminal advancement overwrites terminated →
// completed (and a suspended one appends journal/children post-termination).
func TestCommitAfterTerminateConflicts(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "commit-after-terminate"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	st, err := b.LoadWorkflowHead(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	flipStatusWithoutSweep(t, b, ctx, id)

	// Terminal overwrite attempt must conflict, leaving terminated intact.
	err = b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("terminal commit after terminate: err=%v, want ErrConflict", err)
	}
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "terminated" {
		t.Fatalf("status=%q, want terminated (terminal commit must not overwrite)", inst.Status)
	}

	// Suspended (non-terminal) advancement must also conflict, appending
	// nothing post-termination.
	before, err := b.GetJournal(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents:  []journal.Event{{Seq: st.NextSeq, Type: journal.TypeActivityScheduled, Name: "a"}},
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("suspended commit after terminate: err=%v, want ErrConflict", err)
	}
	after, err := b.GetJournal(ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("journal grew %d→%d post-termination (suspended commit must append nothing)", len(before), len(after))
	}
}
