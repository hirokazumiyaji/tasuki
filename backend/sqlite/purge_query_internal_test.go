package sqlite

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestPurgeVictimQueryHintDecision pins the round-22 P2 hint rule on #294:
// the victim SELECT forces the (completed_at, id) ordering index only for
// broad purges (status sets covering every default purge status), while
// selective purges run unhinted so the planner seeks the visibility index
// instead of walking unrelated old completed rows. Reverting PurgeInstances
// to the unconditional INDEXED BY fails the selective cases below.
func TestPurgeVictimQueryHintDecision(t *testing.T) {
	hinted := []struct {
		name string
		sts  []string
		want bool
	}{
		{"nil (defaults) forces the ordering scan", nil, true},
		{"empty (defaults) forces the ordering scan", []string{}, true},
		{"explicit defaults force the ordering scan", append([]string(nil), backend.DefaultPurgeStatuses...), true},
		{"defaults plus continued forces the ordering scan", append(append([]string(nil), backend.DefaultPurgeStatuses...), "continued"), true},
		{"continued-only runs unhinted", []string{"continued"}, false},
		{"single status runs unhinted", []string{"completed"}, false},
		{"defaults minus canceled runs unhinted", []string{"completed", "failed", "terminated"}, false},
	}
	for _, c := range hinted {
		if got := purgeUsesOrderingHint(c.sts); got != c.want {
			t.Errorf("%s: purgeUsesOrderingHint = %v, want %v", c.name, got, c.want)
		}
		q := purgeVictimQuery(c.sts)
		hasHint := strings.Contains(q, "INDEXED BY wf_instances_completed_at_idx")
		if hasHint != c.want {
			t.Errorf("%s: victim query hint present = %v, want %v\n%s", c.name, hasHint, c.want, q)
		}
		// The generated query must stay a single valid victim SELECT.
		for _, frag := range []string{"SELECT id FROM wf_instances", "WHERE status IN (", "ORDER BY completed_at, id", "LIMIT ?"} {
			if !strings.Contains(q, frag) {
				t.Errorf("%s: victim query lost fragment %q\n%s", c.name, frag, q)
			}
		}
	}
}
