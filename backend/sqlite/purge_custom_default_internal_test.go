package sqlite

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestPurgeHintIgnoresCustomDefaults pins the round-26 P1 hint rule on #294:
// the ordering hint fires only when the requested filter equals the FIXED
// index predicate, never because it equals a customized
// backend.DefaultPurgeStatuses. Fail-without-fix: the old var comparison
// hints ["completed"] under custom defaults and inlines four literals.
func TestPurgeHintIgnoresCustomDefaults(t *testing.T) {
	prev := backend.DefaultPurgeStatuses
	backend.DefaultPurgeStatuses = []string{"completed"}
	t.Cleanup(func() { backend.DefaultPurgeStatuses = prev })

	if purgeUsesOrderingHint([]string{"completed"}) {
		t.Fatal("purgeUsesOrderingHint([completed]) = true under custom defaults, want false (unhinted selective path)")
	}
	if purgeUsesOrderingHint(nil) {
		t.Fatal("purgeUsesOrderingHint(nil) = true under custom defaults [completed], want false (nil normalizes to the custom default)")
	}
	q := purgeVictimQuery([]string{"completed"})
	if strings.Contains(q, "INDEXED BY") {
		t.Fatalf("custom default [completed] victim query must not hint, got:\n%s", q)
	}
	for _, s := range []string{"'failed'", "'terminated'", "'canceled'"} {
		if strings.Contains(q, s) {
			t.Fatalf("custom default query inlines %s, want only 'completed':\n%s", s, q)
		}
	}
	args := purgeVictimStatusArgs([]string{"completed"})
	if len(args) != 1 || args[0] != "completed" {
		t.Fatalf("custom default status args = %v, want [completed]", args)
	}
}
