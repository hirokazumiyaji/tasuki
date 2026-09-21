package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

func TestMigrateIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasuki.db")
	b, err := sqlite.New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
}

func newTestBackend(t *testing.T) (*sqlite.Backend, context.Context) {
	t.Helper()
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "tasuki.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, ctx
}

func appliedVersions(t *testing.T, b *sqlite.Backend, ctx context.Context) []int64 {
	t.Helper()
	rows, err := b.DB().QueryContext(ctx, `SELECT version FROM tasuki_schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func hasColumn(t *testing.T, b *sqlite.Backend, ctx context.Context, table, column string) bool {
	t.Helper()
	rows, err := b.DB().QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return false
}

// TestSchemaVersion verifies the version is 0 before Migrate, reaches the
// embedded latest after Migrate, and stays there (monotonic, idempotent).
func TestSchemaVersion(t *testing.T) {
	b, ctx := newTestBackend(t)
	if v, err := b.SchemaVersion(ctx); err != nil || v != 0 {
		t.Fatalf("before migrate: v=%d err=%v", v, err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	want, err := sqlite.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if want <= 0 {
		t.Fatalf("latest schema version = %d, want > 0", want)
	}
	got, err := b.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("schema version = %d, want %d", got, want)
	}
	// Every embedded migration must be recorded exactly once.
	for i, v := range appliedVersions(t, b, ctx) {
		if v != int64(i+1) {
			t.Fatalf("applied versions = %v, want 1..%d", appliedVersions(t, b, ctx), want)
		}
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != want {
		t.Fatalf("after second migrate: v=%d want=%d err=%v", v, want, err)
	}
}

// TestMigrateLegacyDatabase simulates a database created by the old
// cumulative schema (full tables and columns, but no version bookkeeping):
// Migrate must stamp version 1, tolerate the duplicate-column backfills, and
// reach the latest version.
func TestMigrateLegacyDatabase(t *testing.T) {
	b, ctx := newTestBackend(t)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DROP TABLE tasuki_schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != 0 {
		t.Fatalf("after dropping bookkeeping: v=%d err=%v", v, err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("legacy migrate (duplicate columns must be tolerated): %v", err)
	}
	want, err := sqlite.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != want {
		t.Fatalf("after legacy migrate: v=%d want=%d err=%v", v, want, err)
	}
	if err := b.ValidateSchema(ctx); err != nil {
		t.Fatalf("want valid schema, got %v", err)
	}
}

// TestMigrateLegacyWithoutBackfillColumns simulates a database created before
// the backfill columns existed (no version bookkeeping, heartbeat /
// search_attributes / memo missing): Migrate must add them.
func TestMigrateLegacyWithoutBackfillColumns(t *testing.T) {
	b, ctx := newTestBackend(t)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`ALTER TABLE wf_tasks DROP COLUMN heartbeat`,
		`ALTER TABLE wf_instances DROP COLUMN search_attributes`,
		`ALTER TABLE wf_instances DROP COLUMN memo`,
		`DROP TABLE tasuki_schema_migrations`,
	} {
		if _, err := b.DB().ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][2]string{
		{"wf_tasks", "heartbeat"},
		{"wf_instances", "search_attributes"},
		{"wf_instances", "memo"},
	} {
		if !hasColumn(t, b, ctx, tc[0], tc[1]) {
			t.Fatalf("want column %s.%s backfilled", tc[0], tc[1])
		}
	}
	want, err := sqlite.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != want {
		t.Fatalf("after backfill migrate: v=%d want=%d err=%v", v, want, err)
	}
}

// TestMigratePartialLegacyRepairsBaseline simulates a database created
// before wf_signal_dedupe existed (baseline tables present, the later-added
// table and backfill columns missing, no version bookkeeping): Migrate must
// NOT stamp version 1 and skip the baseline. It runs the idempotent baseline
// DDL to create the missing table, backfills the columns, and leaves a
// valid schema.
func TestMigratePartialLegacyRepairsBaseline(t *testing.T) {
	b, ctx := newTestBackend(t)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Pre-signal-dedupe shape: full baseline minus the later-added table
	// and backfill columns, with version bookkeeping wiped.
	for _, q := range []string{
		`DROP TABLE wf_signal_dedupe`,
		`ALTER TABLE wf_tasks DROP COLUMN heartbeat`,
		`ALTER TABLE wf_instances DROP COLUMN search_attributes`,
		`ALTER TABLE wf_instances DROP COLUMN memo`,
		`DROP TABLE tasuki_schema_migrations`,
	} {
		if _, err := b.DB().ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("partial legacy migrate must repair the baseline, got: %v", err)
	}
	want, err := sqlite.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != want {
		t.Fatalf("after repair migrate: v=%d want=%d err=%v", v, want, err)
	}
	if err := b.ValidateSchema(ctx); err != nil {
		t.Fatalf("want valid schema after repair, got %v", err)
	}
}

// TestMigratePropagatesAlterError verifies that a non-duplicate-column DDL
// failure (here: index creation against a partial table, standing in for
// permission denied and other fatal errors) is returned instead of silently
// ignored.
func TestMigratePropagatesAlterError(t *testing.T) {
	b, ctx := newTestBackend(t)
	// Partial schema: wf_instances exists but with the wrong shape, so the
	// baseline DDL (not the backfill) fails. The baseline is incomplete, so
	// no legacy stamp may be recorded either.
	if _, err := b.DB().ExecContext(ctx,
		`CREATE TABLE wf_instances (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err := b.Migrate(ctx)
	if err == nil {
		t.Fatal("want Migrate to fail when the baseline DDL fails, got nil")
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		t.Fatalf("missing-column failure must not be reported as duplicate column: %v", err)
	}
	// The failed migration must not look applied: nothing may be recorded,
	// so a retry re-runs the baseline from scratch.
	if v, verr := b.SchemaVersion(ctx); verr != nil || v != 0 {
		t.Fatalf("after failed migrate: v=%d want=0 err=%v", v, verr)
	}
}

// TestValidateSchema verifies an unmigrated database is detected, a migrated
// one passes, and a dropped table is reported by name.
func TestValidateSchema(t *testing.T) {
	b, ctx := newTestBackend(t)
	err := b.ValidateSchema(ctx)
	if err == nil {
		t.Fatal("want ValidateSchema to fail on an unmigrated database, got nil")
	}
	for _, table := range []string{"wf_instances", "wf_tasks", "wf_timers"} {
		if !strings.Contains(err.Error(), table) {
			t.Fatalf("want missing tables named, missing %q in: %v", table, err)
		}
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.ValidateSchema(ctx); err != nil {
		t.Fatalf("want valid schema, got %v", err)
	}
	if _, err := b.DB().ExecContext(ctx, `DROP TABLE wf_timers`); err != nil {
		t.Fatal(err)
	}
	err = b.ValidateSchema(ctx)
	if err == nil || !strings.Contains(err.Error(), "wf_timers") {
		t.Fatalf("want missing wf_timers error, got %v", err)
	}
}
