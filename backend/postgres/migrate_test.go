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
