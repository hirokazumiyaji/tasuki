package mysql_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

// TestMigrateCompletedAtIndex verifies the PurgeInstances backing index
// exists after Migrate, and that re-running Migrate on an already-migrated
// database is a no-op (MySQL has no CREATE INDEX IF NOT EXISTS, so the
// duplicate-key error must be tolerated; issue #294, cf. #292).
func TestMigrateCompletedAtIndex(t *testing.T) {
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
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	var n int
	if err := b.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'wf_instances' AND index_name = 'wf_instances_completed_at_idx'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("wf_instances_completed_at_idx missing after Migrate")
	}

	// Round-20 P2: the victim index must lead with both ORDER BY columns
	// (completed_at, id) so the purge victim scan walks victims in order
	// instead of filesorting equal-timestamp groups (parity with the
	// postgres (completed_at, id) index and the sqlite no-TEMP-B-TREE
	// assertion).
	rows, err := b.DB().QueryContext(ctx,
		`SELECT column_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'wf_instances' AND index_name = 'wf_instances_completed_at_idx' ORDER BY seq_in_index`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0] != "completed_at" || cols[1] != "id" {
		t.Fatalf("wf_instances_completed_at_idx columns = %v, want [completed_at id]", cols)
	}
}
