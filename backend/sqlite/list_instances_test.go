package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// The SearchAttributes filter must be evaluated in SQL with LIMIT/OFFSET
// always applied, so filtered listings never read the full table.
func TestListInstancesQueryAppliesLimitWithSAFilter(t *testing.T) {
	q, args := listInstancesQuery(backend.InstanceFilter{
		Status:           "running",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "new"},
		Limit:            5,
		Offset:           10,
	})
	if !strings.Contains(q, "LIMIT ? OFFSET ?") {
		t.Fatalf("SA-filtered query must carry LIMIT/OFFSET:\n%s", q)
	}
	if n := strings.Count(q, "json_each"); n != 2 {
		t.Fatalf("want 2 json_each predicates, got %d:\n%s", n, q)
	}
	// Keys are sorted for deterministic SQL; args follow status/name pairs.
	wantArgs := []any{"running", "running", "", "", "phase", "new", "tenant", "acme", 5, 10}
	if len(args) != len(wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
	for i := range wantArgs {
		if args[i] != wantArgs[i] {
			t.Fatalf("args = %#v, want %#v", args, wantArgs)
		}
	}

	// No filter: LIMIT/OFFSET still present, no JSON predicates.
	q, args = listInstancesQuery(backend.InstanceFilter{})
	if !strings.Contains(q, "LIMIT ? OFFSET ?") {
		t.Fatalf("unfiltered query must carry LIMIT/OFFSET:\n%s", q)
	}
	if strings.Contains(q, "json_each") {
		t.Fatalf("unfiltered query must not carry JSON predicates:\n%s", q)
	}
	if len(args) != 6 || args[4] != 100 || args[5] != 0 {
		t.Fatalf("default limit/offset args = %#v", args)
	}
}

// EXPLAIN must show the engine enforcing LIMIT (OffsetLimit opcode) even when
// a SearchAttributes filter is present.
func TestListInstancesQueryExplainShowsLimit(t *testing.T) {
	ctx := context.Background()
	b, err := New(filepath.Join(t.TempDir(), "tasuki.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	q, args := listInstancesQuery(backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme"},
		Limit:            5,
		Offset:           10,
	})
	rows, err := b.db.QueryContext(ctx, "EXPLAIN "+q, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for _, v := range vals {
			if s, ok := v.(string); ok && s == "OffsetLimit" {
				found = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("EXPLAIN of SA-filtered ListInstances query shows no OffsetLimit opcode: LIMIT not applied")
	}
}
