package sqlite

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestPurgeVictimQueryHintDecision pins the round-23 P2 hint rule on #294:
// the victim SELECT forces the (completed_at, id) ordering index only for
// the default purge (status sets equal to backend.DefaultPurgeStatuses),
// while every other filter runs unhinted: non-default statuses such as
// "continued" are not in the partial index (forcing it would miss victims),
// and selective subsets seek the visibility index instead of walking
// unrelated default-status rows. Reverting PurgeInstances to the
// unconditional INDEXED BY fails the unhinted cases below; reverting to the
// round-22 superset rule fails the defaults-plus-continued case.
func TestPurgeVictimQueryHintDecision(t *testing.T) {
	hinted := []struct {
		name string
		sts  []string
		want bool
	}{
		{"nil (defaults) forces the ordering scan", nil, true},
		{"empty (defaults) forces the ordering scan", []string{}, true},
		{"explicit defaults force the ordering scan", append([]string(nil), backend.DefaultPurgeStatuses...), true},
		{"reordered defaults force the ordering scan", []string{"canceled", "terminated", "failed", "completed"}, true},
		{"defaults plus continued runs unhinted (continued not in the victim index)", append(append([]string(nil), backend.DefaultPurgeStatuses...), "continued"), false},
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
		// The hinted query must carry no status placeholders: INDEXED BY
		// a partial index is a prepare-time "no query solution" error
		// unless the WHERE clause provably implies the index predicate,
		// which bind parameters cannot satisfy — so the default set is
		// inlined as literals (see purgeVictimQuery). The unhinted query
		// keeps one placeholder per status.
		args := purgeVictimStatusArgs(c.sts)
		if c.want {
			if len(args) != 0 {
				t.Errorf("%s: hinted victim status args = %v, want none (default set inlined)", c.name, args)
			}
			if strings.Contains(q, "? AND completed_at") || strings.Contains(q, "?,") {
				t.Errorf("%s: hinted victim query still binds statuses:\n%s", c.name, q)
			}
			// The inlined literals must be exactly the default set: the
			// partial index predicate is only provable (and the scan only
			// correct) when the filter matches the index contents.
			for _, s := range backend.DefaultPurgeStatuses {
				if !strings.Contains(q, "'"+s+"'") {
					t.Errorf("%s: hinted victim query lost literal %q:\n%s", c.name, s, q)
				}
			}
		} else if len(args) != len(c.sts) {
			t.Errorf("%s: unhinted victim status args = %d, want %d (one per status)", c.name, len(args), len(c.sts))
		}
	}
}
