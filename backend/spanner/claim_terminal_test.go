package spanner

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// A Limit:1 poll head-blocked by a terminal-instance task must still reach
// the live task behind it (Codex round 2 on #327). ClaimTasks deletes the
// stale row best-effort and reselects into the freed slot; without that the
// same stale candidate is returned on every call and live tasks starve.
func TestClaimDeletesTerminalTaskAndRefills(t *testing.T) {
	dsn := guardTestDSN(t)
	ctx := context.Background()
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

	const liveID, termID = "claim-term-live", "claim-term-dead"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: liveID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: termID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, termID); err != nil {
		t.Fatal(err)
	}
	// Re-insert a stale task for the terminated instance, older than every
	// other row so it head-blocks the visible_at-ordered claim select.
	staleAt := time.Now().UTC().Add(-time.Hour)
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_tasks", map[string]any{
				"id": newID(), "kind": "workflow", "queue": "default",
				"instance_id": termID, "attempt": int64(0),
				"visible_at": staleAt, "created_at": staleAt,
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "claim-term",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].InstanceID != liveID {
		t.Fatalf("Limit:1 claim behind a terminal task returned %v (want the live task)", tasks)
	}
	n, err := b.countTasksForInstance(ctx, termID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("stale terminal task was skipped but not deleted (%d remain)", n)
	}
}

func (b *Backend) countTasksForInstance(ctx context.Context, id string) (int64, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT COUNT(*) FROM wf_tasks WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return 0, err
	}
	var n int64
	if err := row.Columns(&n); err != nil {
		return 0, err
	}
	return n, nil
}
