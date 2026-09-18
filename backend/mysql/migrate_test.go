package mysql_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TASUKI_MYSQL_DSN not set")
	}
	return dsn
}

// resetSchema drops every tasuki table (including migration bookkeeping) so a
// test can exercise Migrate from a genuinely fresh database. The shared test
// database is stateful across tests, so migration-related tests reset first
// and leave the database migrated for the next test.
func resetSchema(t *testing.T, b *mysql.Backend) {
	t.Helper()
	ctx := context.Background()
	db := b.DB()
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{
		"tasuki_schema_migrations",
		"wf_schedules", "wf_timers", "wf_tasks", "wf_signal_dedupe",
		"wf_inbox", "wf_journal", "wf_instances",
	} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

func hasColumn(t *testing.T, b *mysql.Backend, ctx context.Context, table, column string) bool {
	t.Helper()
	var n int64
	if err := b.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func TestMigrateIdempotent(t *testing.T) {
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
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestSchemaVersion verifies the version is 0 before Migrate, reaches the
// embedded latest after Migrate, and stays there (monotonic, idempotent).
func TestSchemaVersion(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	resetSchema(t, b)
	t.Cleanup(func() {
		resetSchema(t, b)
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	})
	if v, err := b.SchemaVersion(ctx); err != nil || v != 0 {
		t.Fatalf("before migrate: v=%d err=%v", v, err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	want, err := mysql.LatestSchemaVersion()
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
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	resetSchema(t, b)
	t.Cleanup(func() {
		resetSchema(t, b)
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	})
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB().ExecContext(ctx, `DROP TABLE tasuki_schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("legacy migrate (duplicate columns must be tolerated): %v", err)
	}
	want, err := mysql.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v != want {
		t.Fatalf("after legacy migrate: v=%d want=%d err=%v", v, want, err)
	}
}

// TestMigrateLegacyWithoutBackfillColumns simulates a database created before
// the backfill columns existed (no version bookkeeping, heartbeat /
// search_attributes / memo missing): Migrate must add them.
func TestMigrateLegacyWithoutBackfillColumns(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	resetSchema(t, b)
	t.Cleanup(func() {
		resetSchema(t, b)
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	})
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
}

// TestMigratePropagatesAlterError verifies that a non-duplicate-column ALTER
// failure (here: missing table, standing in for permission denied and other
// fatal errors) is returned instead of silently ignored.
func TestMigratePropagatesAlterError(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	resetSchema(t, b)
	t.Cleanup(func() {
		resetSchema(t, b)
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	})
	// Legacy-shaped DB: wf_instances exists (so Migrate stamps version 1 and
	// skips the baseline DDL) but wf_tasks is missing, so the backfill ALTER
	// fails with "table doesn't exist".
	if _, err := b.DB().ExecContext(ctx,
		`CREATE TABLE wf_instances (id VARCHAR(255) PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err = b.Migrate(ctx)
	if err == nil {
		t.Fatal("want Migrate to fail when the backfill ALTER fails, got nil")
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		t.Fatalf("missing-table failure must not be reported as duplicate column: %v", err)
	}
	// The failed migration must not look applied: only the legacy stamp (v1)
	// may be recorded, so a retry re-runs the backfill.
	if v, verr := b.SchemaVersion(ctx); verr != nil || v != 1 {
		t.Fatalf("after failed migrate: v=%d want=1 err=%v", v, verr)
	}
}

// TestValidateSchema verifies an unmigrated database is detected, a migrated
// one passes, and a dropped table is reported by name.
func TestValidateSchema(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	resetSchema(t, b)
	t.Cleanup(func() {
		resetSchema(t, b)
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	})
	err = b.ValidateSchema(ctx)
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
