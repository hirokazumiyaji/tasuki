package mysql

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// The SearchAttributes filter must be evaluated in SQL with LIMIT/OFFSET
// always applied, so filtered listings never read the full table.
// (No live DB needed: this only inspects the generated SQL and args.)
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
	if n := strings.Count(q, "JSON_CONTAINS"); n != 2 {
		t.Fatalf("want 2 JSON_CONTAINS predicates, got %d:\n%s", n, q)
	}
	if n := strings.Count(q, "JSON_OBJECT(?, ?)"); n != 2 {
		t.Fatalf("want 2 JSON_OBJECT(?, ?) predicates, got %d:\n%s", n, q)
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
	if strings.Contains(q, "JSON_CONTAINS") {
		t.Fatalf("unfiltered query must not carry JSON predicates:\n%s", q)
	}
	if len(args) != 6 || args[4] != 100 || args[5] != 0 {
		t.Fatalf("default limit/offset args = %#v", args)
	}
}
