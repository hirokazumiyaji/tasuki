package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// TestPurgeRespectsCustomDefaultStatuses is the round-26 P1 regression test
// on #294: customizing backend.DefaultPurgeStatuses to ["completed"] and
// purging with a nil filter must delete only completed instances. The pre-fix
// rule compared the normalized filter against the customizable var, so
// ["completed"] hinted the default partial index and the victim SELECT
// inlined the four hard-coded statuses — deleting failed/terminated/canceled
// too. Fail-without-fix: revert purgeUsesOrderingHint to compare against
// backend.DefaultPurgeStatuses and this fails (n=4, survivors gone).
func TestPurgeRespectsCustomDefaultStatuses(t *testing.T) {
	prev := backend.DefaultPurgeStatuses
	backend.DefaultPurgeStatuses = []string{"completed"}
	t.Cleanup(func() { backend.DefaultPurgeStatuses = prev })

	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_custom.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	old := time.Now().UTC().Add(-2 * time.Hour).Format("2006-01-02T15:04:05.000000000Z")
	for _, st := range []string{"completed", "failed", "terminated", "canceled"} {
		if _, err := b.DB().ExecContext(ctx,
			`INSERT INTO wf_instances (id, name, queue, status, next_seq, created_at, updated_at, completed_at) VALUES (?, ?, ?, ?, 2, ?, ?, ?)`,
			"purge-"+st, "wf", "default", st, old, old, old); err != nil {
			t.Fatal(err)
		}
	}

	n, err := b.PurgeInstances(ctx, time.Hour, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("PurgeInstances(nil) with custom defaults [completed] deleted %d, want 1", n)
	}
	remaining := map[string]bool{}
	rows, err := b.DB().QueryContext(ctx, `SELECT id FROM wf_instances`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		remaining[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"purge-failed", "purge-terminated", "purge-canceled"} {
		if !remaining[want] {
			t.Fatalf("custom-default purge deleted %s, want it to survive", want)
		}
	}
	if remaining["purge-completed"] {
		t.Fatal("custom-default purge left purge-completed behind, want it deleted")
	}
}
