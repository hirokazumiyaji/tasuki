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
	// it from Migrate. Under versioned migrations a plain re-Migrate skips
	// already-applied versions, so simulate an unapplied 000003 by dropping
	// the index and deleting its version row.
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version = 3`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasIndex() {
		t.Fatal("wf_instances_completed_at_idx not recreated by re-Migrate")
	}
}

// TestPurgeIndexLeadsWithOrderingColumns guards the round-19 P2 index
// shape: the victim index must lead with (completed_at, id) so the ordered
// scan serves ORDER BY without a TEMP B-TREE sort, and the 000004 migration
// must retrofit databases still carrying the pre-fix (status, completed_at)
// shape (databases at version >= 3 never re-run 000003).
func TestPurgeIndexLeadsWithOrderingColumns(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_shape.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	indexSQL := func() string {
		var sql string
		if err := b.DB().QueryRowContext(ctx,
			`SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'wf_instances_completed_at_idx'`,
		).Scan(&sql); err != nil {
			t.Fatal(err)
		}
		return sql
	}
	if sql := indexSQL(); !strings.Contains(sql, "(completed_at, id)") {
		t.Fatalf("victim index DDL = %q, want leading (completed_at, id)", sql)
	}
	// Simulate a pre-fix database: stale status-leading index with 000004
	// unapplied, then re-Migrate must rebuild the ordering-leading shape.
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx,
		`CREATE INDEX wf_instances_completed_at_idx ON wf_instances (status, completed_at) WHERE completed_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version = 4`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if sql := indexSQL(); !strings.Contains(sql, "(completed_at, id)") {
		t.Fatalf("retrofitted victim index DDL = %q, want leading (completed_at, id)", sql)
	}
}
// through EXPLAIN QUERY PLAN and requires an index-backed search (issue #294).
// Round-19 P2 additionally requires NO temp b-tree: the victim index leads
// with (completed_at, id) and the query forces it, so the ordered scan
// serves the ORDER BY directly instead of sorting.
// TestPurgeVictimScanUsesIndex runs the exact PurgeInstances victim SELECT
// through EXPLAIN QUERY PLAN and requires an index-backed search (issue #294).
// Round-19 P2 additionally requires NO temp b-tree: the victim index leads
// with (completed_at, id) and the query forces it, so the ordered scan
// serves the ORDER BY directly instead of sorting.
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
		SELECT id FROM wf_instances INDEXED BY wf_instances_completed_at_idx
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
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("purge victim scan sorts via TEMP B-TREE (index must lead with the ORDER BY columns):\n%s", joined)
	}
}

// TestPurgeSelectiveScanAvoidsForcedOrdering covers the round-22 P2 on
// #294: a selective purge (e.g. statuses=["continued"]) must NOT force the
// (completed_at, id) ordering index. The forced scan walks unrelated old
// completed rows on every call; the unhinted planner seeks the
// (status, ...) visibility index and sorts only the few matches. The plan
// below is the exact unhinted query PurgeInstances issues for a selective
// status set (see purgeVictimQuery): it must be a visibility-index SEARCH
// (never a full-table SCAN), must not touch the forced ordering index, and
// the ORDER BY is served by a small TEMP B-TREE sort over the selective
// matches rather than a full-order walk.
func TestPurgeSelectiveScanAvoidsForcedOrdering(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_selective_plan.db"))
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
		WHERE status IN ('continued')
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
	if !strings.Contains(joined, "SEARCH") {
		t.Fatalf("selective purge victim scan is not a SEARCH:\n%s", joined)
	}
	if strings.Contains(joined, "SCAN wf_instances") {
		t.Fatalf("selective purge victim scan walks the table instead of seeking an index:\n%s", joined)
	}
	if !strings.Contains(joined, "USING INDEX wf_instances_visibility_idx") {
		t.Fatalf("selective purge victim scan does not seek the visibility index:\n%s", joined)
	}
	if strings.Contains(joined, "wf_instances_completed_at_idx") {
		t.Fatalf("selective purge victim scan uses the forced ordering index (must run unhinted):\n%s", joined)
	}
}
