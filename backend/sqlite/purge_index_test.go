package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// TestPurgeCompletedAtIndex verifies the completed_at indexes backing
// PurgeInstances exist after Migrate (issue #294, continued index round-24).
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

	hasIndex := func(name string) bool {
		var n int
		if err := b.DB().QueryRowContext(ctx,
			`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, name,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	if !hasIndex("wf_instances_completed_at_idx") {
		t.Fatal("wf_instances_completed_at_idx missing after Migrate")
	}
	if !hasIndex("wf_instances_continued_purge_idx") {
		t.Fatal("wf_instances_continued_purge_idx missing after Migrate")
	}

	// Retrofit path: databases provisioned before the indexes existed must
	// gain them from Migrate. Under versioned migrations a plain re-Migrate
	// skips already-applied versions, so simulate unapplied 000005/000006 by
	// dropping the indexes and deleting their version rows.
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_continued_purge_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version IN (5, 6)`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasIndex("wf_instances_completed_at_idx") {
		t.Fatal("wf_instances_completed_at_idx not recreated by re-Migrate")
	}
	if !hasIndex("wf_instances_continued_purge_idx") {
		t.Fatal("wf_instances_continued_purge_idx not recreated by re-Migrate")
	}
}

// TestPurgeIndexLeadsWithOrderingColumns guards the round-19 P2 index
// shape, restricted round-23 to the default purge statuses and extended
// round-24 with the continued-only ordered path: the default victim index
// must lead with (completed_at, id) so the ordered scan serves ORDER BY
// without a TEMP B-TREE sort, and its partial predicate must admit exactly
// backend.DefaultPurgeStatuses so the forced default scan never walks old
// continued rows; the continued victim index must lead with the same
// ordering and admit exactly status = 'continued'. The 000004 migration must
// retrofit databases still carrying the pre-fix (status, completed_at)
// shape, 000005 the pre-restriction (completed_at-only predicate) shape,
// and 000006 the missing continued index (databases at version >= 6 never
// re-run the earlier versions).
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
	// The partial predicate must admit exactly the default purge statuses:
	// continued rows (never deleted by a default purge) must not be in the
	// index, or the forced ordered scan walks them on every batch.
	if sql := indexSQL(); !strings.Contains(sql, "completed_at IS NOT NULL") {
		t.Fatalf("victim index DDL = %q, want completed_at IS NOT NULL predicate", sql)
	}
	for _, s := range []string{"'completed'", "'failed'", "'terminated'", "'canceled'"} {
		if sql := indexSQL(); !strings.Contains(sql, s) {
			t.Fatalf("victim index DDL = %q, want default status %s in the predicate", sql, s)
		}
	}
	if sql := indexSQL(); strings.Contains(sql, "'continued'") {
		t.Fatalf("victim index DDL = %q, must not admit continued rows", sql)
	}
	// Simulate a pre-fix database: stale status-leading index with 000004
	// and 000005 unapplied, then re-Migrate must rebuild the
	// ordering-leading, default-status shape through both migrations.
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx,
		`CREATE INDEX wf_instances_completed_at_idx ON wf_instances (status, completed_at) WHERE completed_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version IN (4, 5)`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if sql := indexSQL(); !strings.Contains(sql, "(completed_at, id)") {
		t.Fatalf("retrofitted victim index DDL = %q, want leading (completed_at, id)", sql)
	}
	if sql := indexSQL(); strings.Contains(sql, "'continued'") || !strings.Contains(sql, "'completed'") {
		t.Fatalf("retrofitted victim index DDL = %q, want exactly the default-status predicate", sql)
	}
	// Simulate a pre-restriction database: stale completed_at-only
	// predicate with 000005 unapplied, then re-Migrate must rebuild the
	// default-status shape.
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_completed_at_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx,
		`CREATE INDEX wf_instances_completed_at_idx ON wf_instances (completed_at, id) WHERE completed_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version = 5`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if sql := indexSQL(); !strings.Contains(sql, "(completed_at, id)") {
		t.Fatalf("retrofitted victim index DDL = %q, want leading (completed_at, id)", sql)
	}
	if sql := indexSQL(); strings.Contains(sql, "'continued'") || !strings.Contains(sql, "'completed'") {
		t.Fatalf("retrofitted victim index DDL = %q, want exactly the default-status predicate", sql)
	}
	// The continued victim index (round-24 P2) must exist with the ordered
	// shape and exactly the continued predicate; dropping it with 000006
	// unapplied must recreate it via re-Migrate.
	indexContinuedSQL := func() string {
		var sql string
		if err := b.DB().QueryRowContext(ctx,
			`SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'wf_instances_continued_purge_idx'`,
		).Scan(&sql); err != nil {
			t.Fatal(err)
		}
		return sql
	}
	if sql := indexContinuedSQL(); !strings.Contains(sql, "(completed_at, id)") {
		t.Fatalf("continued victim index DDL = %q, want leading (completed_at, id)", sql)
	}
	if sql := indexContinuedSQL(); !strings.Contains(sql, "completed_at IS NOT NULL") || !strings.Contains(sql, "'continued'") {
		t.Fatalf("continued victim index DDL = %q, want completed_at IS NOT NULL AND status = 'continued'", sql)
	}
	for _, s := range []string{"'completed'", "'failed'", "'terminated'", "'canceled'"} {
		if sql := indexContinuedSQL(); strings.Contains(sql, s) {
			t.Fatalf("continued victim index DDL = %q, must not admit default status %s", sql, s)
		}
	}
	if _, err := b.DB().ExecContext(ctx, `DROP INDEX wf_instances_continued_purge_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DELETE FROM tasuki_schema_migrations WHERE version = 6`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if sql := indexContinuedSQL(); !strings.Contains(sql, "(completed_at, id)") || !strings.Contains(sql, "'continued'") {
		t.Fatalf("retrofitted continued victim index DDL = %q, want ordered continued shape", sql)
	}
}
// through EXPLAIN QUERY PLAN and requires an index-backed search (issue #294).
// Round-19 P2 additionally requires NO temp b-tree: the victim index leads
// with (completed_at, id) and the query forces it, so the ordered scan
// serves the ORDER BY directly instead of sorting.
// TestPurgeVictimScanUsesIndex runs the exact default PurgeInstances victim
// SELECT through EXPLAIN QUERY PLAN and requires an index-backed search
// (issue #294). Round-19 P2 additionally requires NO temp b-tree: the
// victim index leads with (completed_at, id) and the query forces it, so
// the ordered scan serves the ORDER BY directly instead of sorting.
// Round-23 uses the full default 4-status filter: the partial index admits
// exactly those statuses, so the forced scan is applicable and walks
// victims only (no continued rows are in the index).
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
		WHERE status IN ('completed', 'failed', 'terminated', 'canceled')
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
// #294: a selective purge (e.g. statuses=["completed"]) must NOT force an
// ordering index. The forced scan walks unrelated old rows on every call;
// the unhinted planner seeks the (status, ...) visibility index and sorts
// only the few matches. The plan below is the exact unhinted query
// PurgeInstances issues for a selective status set (see purgeVictimQuery):
// it must be a visibility-index SEARCH (never a full-table SCAN), must not
// touch either forced ordering index, and the ORDER BY is served by a small
// TEMP B-TREE sort over the selective matches rather than a full-order
// walk. (Continued-only used to be the example here; round-24 P2 gives it
// its own ordered partial index — see TestPurgeContinuedScanUsesIndex.)
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
		WHERE status IN ('completed')
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
	if strings.Contains(joined, "wf_instances_continued_purge_idx") {
		t.Fatalf("selective purge victim scan uses the continued ordering index (must run unhinted):\n%s", joined)
	}
}

// TestPurgeContinuedScanUsesIndex runs the exact continued-only
// PurgeInstances victim SELECT through EXPLAIN QUERY PLAN and requires the
// round-24 P2 ordered path (issue #294): the continued partial index leads
// with (completed_at, id) and the query forces it, so the ordered scan
// serves the ORDER BY directly instead of sorting via visibility_idx + TEMP
// B-TREE. Dropping the continued hint (or the index) reintroduces the TEMP
// B-TREE sort and this fails.
func TestPurgeContinuedScanUsesIndex(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "purge_continued_plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := b.DB().QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT id FROM wf_instances INDEXED BY wf_instances_continued_purge_idx
		WHERE status = 'continued'
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
	if !strings.Contains(joined, "wf_instances_continued_purge_idx") {
		t.Fatalf("continued purge victim scan does not use wf_instances_continued_purge_idx:\n%s", joined)
	}
	if !strings.Contains(joined, "SEARCH") {
		t.Fatalf("continued purge victim scan is not a SEARCH:\n%s", joined)
	}
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("continued purge victim scan sorts via TEMP B-TREE (index must lead with the ORDER BY columns):\n%s", joined)
	}
	if strings.Contains(joined, "wf_instances_visibility_idx") {
		t.Fatalf("continued purge victim scan seeks the visibility index (must walk the ordered continued index):\n%s", joined)
	}
}
