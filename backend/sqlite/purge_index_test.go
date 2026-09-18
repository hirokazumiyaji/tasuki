package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// TestPurgeCompletedAtIndex verifies the completed_at index backing
// PurgeInstances exists after Migrate (issue #294).
func TestPurgeCompletedAtIndex(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_idx.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	hasIndex := func() bool {
		var n int
		if err := b.DB().QueryRowContext(ctx,
			`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'wf_instances_completed_at_idx'`,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	if !hasIndex() {
		t.Fatal("wf_instances_completed_at_idx missing after Migrate")
	}

	// Retrofit path: databases provisioned before the index existed must gain
	// it from a plain re-Migrate (no versioned migrations on SQLite yet, #292).
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasIndex() {
		t.Fatal("wf_instances_completed_at_idx not recreated by re-Migrate")
	}
}

// TestPurgeVictimScanUsesIndex runs the exact PurgeInstances victim SELECT
// through EXPLAIN QUERY PLAN and requires an index-backed search (issue #294).
func TestPurgeVictimScanUsesIndex(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := b.DB().QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT id FROM wf_instances
		WHERE status IN ('completed', 'failed', 'terminated')
		  AND completed_at IS NOT NULL AND completed_at <= '2026-01-01T00:00:00.000000000Z'
		ORDER BY completed_at, id
		LIMIT 100`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "USING INDEX wf_instances_completed_at_idx") {
		t.Fatalf("purge victim scan does not use wf_instances_completed_at_idx:\n%s", joined)
	}
	if !strings.Contains(joined, "SEARCH") {
		t.Fatalf("purge victim scan is not a SEARCH:\n%s", joined)
	}
}
