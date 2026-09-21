package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

// TestFairClaimRefillsPastLockedHead covers the issue #294 P1: when a
// concurrent claimer holds locks on every head row the picker accepts, the
// claim must refill from later candidates instead of returning an empty
// batch while claimable tasks remain.
func TestFairClaimRefillsPastLockedHead(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
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

	const instances = 4
	for i := 0; i < instances; i++ {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: fmt.Sprintf("refill-%d", i), Name: "WF", Queue: "refill",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Blocker holds locks on the two head rows, like a concurrent fair
	// claimer that scanned the same IDs and locked them first.
	btx, err := b.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback()
	headRows, err := btx.QueryContext(ctx, `
		SELECT id FROM wf_tasks
		WHERE kind = 'workflow' AND queue = 'refill'
		ORDER BY visible_at, id
		LIMIT 2
		FOR UPDATE`)
	if err != nil {
		t.Fatal(err)
	}
	head := map[int64]bool{}
	for headRows.Next() {
		var id int64
		if err := headRows.Scan(&id); err != nil {
			headRows.Close()
			t.Fatal(err)
		}
		head[id] = true
	}
	headRows.Close()
	if err := headRows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(head) != 2 {
		t.Fatalf("blocked %d head rows, want 2", len(head))
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"refill"}, Limit: 2,
		Lease: time.Minute, WorkerID: "refill-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("claimed %d tasks behind locked head rows, want 2", len(tasks))
	}
	for _, task := range tasks {
		if head[task.ID] {
			t.Fatalf("claimed locked head task %d", task.ID)
		}
	}
}
