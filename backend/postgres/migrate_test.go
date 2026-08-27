package postgres_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/postgres"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	return dsn
}

// resetSchema wipes every tasuki table (including migration bookkeeping) so a
// test can exercise Migrate from a genuinely fresh database. The shared test
// database is stateful across tests, so migration-related tests reset first.
func resetSchema(t *testing.T, b *postgres.Backend) {
	t.Helper()
	ctx := context.Background()
	if _, err := b.Pool().Exec(ctx, `
		DROP TABLE IF EXISTS tasuki_schema_migrations,
			wf_schedules, wf_timers, wf_tasks, wf_signal_dedupe,
			wf_inbox, wf_journal, wf_instances CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func TestMigrate(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestMigrateAutovacuumTuning verifies that Migrate applies the retention /
// HOT-friendly reloptions to the high-churn tables (issue #255).
func TestMigrateAutovacuumTuning(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := b.Pool().Query(ctx, `
		SELECT relname, reloptions FROM pg_class
		WHERE relname IN ('wf_tasks', 'wf_inbox', 'wf_instances', 'wf_signal_dedupe')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var name string
		var opts []string
		if err := rows.Scan(&name, &opts); err != nil {
			t.Fatal(err)
		}
		for _, o := range opts {
			got[name+","+o] = o
		}
		got[name] = strings.Join(opts, ",")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"wf_tasks":         {"autovacuum_vacuum_scale_factor=0.05", "autovacuum_vacuum_cost_limit=1000", "fillfactor=80"},
		"wf_inbox":         {"autovacuum_vacuum_scale_factor=0.05", "autovacuum_vacuum_cost_limit=1000"},
		"wf_instances":     {"autovacuum_vacuum_scale_factor=0.05", "fillfactor=80"},
		"wf_signal_dedupe": {"autovacuum_vacuum_scale_factor=0.05"},
	}
	for table, opts := range want {
		for _, o := range opts {
			if !strings.Contains(got[table], o) {
				t.Fatalf("%s reloptions = %q, want to contain %q", table, got[table], o)
			}
		}
	}
}

func TestSchemaVersion(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	resetSchema(t, b)
	if v, err := b.SchemaVersion(ctx); err != nil || v != 0 {
		t.Fatalf("before migrate: v=%d err=%v", v, err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	want, err := postgres.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("schema version = %d, want %d", got, want)
	}
}

// TestMigrateLegacyDatabase simulates a database created by the old
// cumulative schema.sql (schema present, no version bookkeeping): Migrate
// must stamp version 1 and apply the remaining migrations.
func TestMigrateLegacyDatabase(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	resetSchema(t, b)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Pool().Exec(ctx, `DELETE FROM tasuki_schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	v, err := b.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want, err := postgres.LatestSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v != want {
		t.Fatalf("after legacy stamp v=%d, want %d", v, want)
	}
}

func TestValidateSchema(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// Fresh database: Migrate makes validation pass; dropping a table makes
	// it fail with the missing table named. Re-running migrations from
	// scratch (bookkeeping wiped) repairs the schema.
	resetSchema(t, b)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.ValidateSchema(ctx); err != nil {
		t.Fatalf("want valid schema, got %v", err)
	}
	if _, err := b.Pool().Exec(ctx, `DROP TABLE wf_timers`); err != nil {
		t.Fatal(err)
	}
	err = b.ValidateSchema(ctx)
	if err == nil || !strings.Contains(err.Error(), "wf_timers") {
		t.Fatalf("want missing wf_timers error, got %v", err)
	}
	if v, err := b.SchemaVersion(ctx); err != nil || v == 0 {
		t.Fatalf("version bookkeeping should survive the drop: v=%d err=%v", v, err)
	}
	resetSchema(t, b)
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("repair migrate: %v", err)
	}
	if err := b.ValidateSchema(ctx); err != nil {
		t.Fatalf("want repaired schema, got %v", err)
	}
}
