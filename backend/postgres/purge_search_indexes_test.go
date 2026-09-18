package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/postgres"
)

// TestMigratePurgeSearchIndexes verifies migration 000003 (issue #294): the
// purge and search_attributes indexes exist and EXPLAIN shows the purge
// victim SELECT and the @> listing filter using them instead of sequential
// scans.
func TestMigratePurgeSearchIndexes(t *testing.T) {
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

	for _, want := range []string{
		"wf_instances_completed_at_idx",
		"wf_instances_search_attributes_gin_idx",
	} {
		var n int
		if err := b.Pool().QueryRow(ctx,
			`SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`, want,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("index %s missing after Migrate", want)
		}
	}

	// EXPLAIN on near-empty tables would seqscan by cost; disable seqscan so
	// the plan proves the indexes are *usable* for these query shapes. Seed a
	// few rows and ANALYZE so the planner has stats, all inside a rolled-back
	// transaction to leave the shared test database untouched.
	explain := func(query string, args ...any) string {
		t.Helper()
		tx, err := b.Pool().Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO wf_instances (id, name, status, search_attributes, completed_at)
			SELECT 'seed-' || g, 'WF', (ARRAY['completed', 'failed', 'running'])[1 + g % 3],
			       ('{"tier":"gold","n":' || g || '}')::jsonb,
			       CASE WHEN g % 3 != 2 THEN now() - (g || ' hours')::interval END
			FROM generate_series(1, 9) g`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `ANALYZE wf_instances`); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var sb strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			sb.WriteString(line)
			sb.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return sb.String()
	}

	purgePlan := explain(`
		EXPLAIN SELECT id FROM wf_instances
		WHERE status = ANY($1)
		  AND completed_at IS NOT NULL
		  AND completed_at <= now() - $2::interval
		ORDER BY completed_at, id
		LIMIT $3`,
		[]string{"completed", "failed", "terminated"}, "1 hour", 100)
	if strings.Contains(purgePlan, "Seq Scan") {
		t.Fatalf("purge victim SELECT seqscans:\n%s", purgePlan)
	}
	if !strings.Contains(purgePlan, "wf_instances_completed_at_idx") {
		t.Fatalf("purge victim SELECT does not use wf_instances_completed_at_idx:\n%s", purgePlan)
	}

	saPlan := explain(`
		EXPLAIN SELECT id FROM wf_instances
		WHERE search_attributes @> $1::jsonb
		ORDER BY created_at, id
		LIMIT 100`, `{"tier":"gold"}`)
	if strings.Contains(saPlan, "Seq Scan") {
		t.Fatalf("search_attributes listing seqscans:\n%s", saPlan)
	}
	if !strings.Contains(saPlan, "wf_instances_search_attributes_gin_idx") {
		t.Fatalf("search_attributes listing does not use wf_instances_search_attributes_gin_idx:\n%s", saPlan)
	}
}
