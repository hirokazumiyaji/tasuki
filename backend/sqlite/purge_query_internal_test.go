package sqlite

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestPurgeVictimQueryHintDecision pins the round-23 P2 hint rule on #294,
// extended round-24 P2 with the continued-only ordered path: the victim
// SELECT forces the (completed_at, id) default partial index for the default
// purge, the continued partial index for statuses=["continued"], while every
// other filter runs unhinted: mixed sets such as defaults-plus-continued are
// in neither partial index (forcing either would miss victims), and
// selective subsets seek the visibility index instead of walking unrelated
// default-status rows. Reverting PurgeInstances to the unconditional
// INDEXED BY fails the unhinted cases below; reverting to the round-22
// superset rule fails the defaults-plus-continued case; dropping the
// continued hint reintroduces the round-24 TEMP B-TREE sort for continued
// purges.
func TestPurgeVictimQueryHintDecision(t *testing.T) {
	hinted := []struct {
		name string
		sts  []string
		want string // "" = unhinted, else the forced index name
	}{
		{"nil (defaults) forces the default ordering scan", nil, "wf_instances_completed_at_idx"},
		{"empty (defaults) forces the default ordering scan", []string{}, "wf_instances_completed_at_idx"},
		{"explicit defaults force the default ordering scan", append([]string(nil), backend.DefaultPurgeStatuses...), "wf_instances_completed_at_idx"},
		{"reordered defaults force the default ordering scan", []string{"canceled", "terminated", "failed", "completed"}, "wf_instances_completed_at_idx"},
		{"defaults plus continued runs unhinted (in neither partial index)", append(append([]string(nil), backend.DefaultPurgeStatuses...), "continued"), ""},
		{"continued-only forces the continued ordering scan", []string{"continued"}, "wf_instances_continued_purge_idx"},
		{"single status runs unhinted", []string{"completed"}, ""},
		{"defaults minus canceled runs unhinted", []string{"completed", "failed", "terminated"}, ""},
	}
	for _, c := range hinted {
		wantDefault := c.want == "wf_instances_completed_at_idx"
		if got := purgeUsesOrderingHint(c.sts); got != wantDefault {
			t.Errorf("%s: purgeUsesOrderingHint = %v, want %v", c.name, got, wantDefault)
		}
		wantContinued := c.want == "wf_instances_continued_purge_idx"
		if got := purgeContinuedOrderingHint(c.sts); got != wantContinued {
			t.Errorf("%s: purgeContinuedOrderingHint = %v, want %v", c.name, got, wantContinued)
		}
		q := purgeVictimQuery(c.sts)
		for _, idx := range []string{"wf_instances_completed_at_idx", "wf_instances_continued_purge_idx"} {
			hasHint := strings.Contains(q, "INDEXED BY "+idx)
			if hasHint != (c.want == idx) {
				t.Errorf("%s: victim query hint INDEXED BY %s present = %v, want %v\n%s", c.name, idx, hasHint, c.want == idx, q)
			}
		}
		// The generated query must stay a single valid victim SELECT.
		for _, frag := range []string{"SELECT id FROM wf_instances", "WHERE status", "ORDER BY completed_at, id", "LIMIT ?"} {
			if !strings.Contains(q, frag) {
				t.Errorf("%s: victim query lost fragment %q\n%s", c.name, frag, q)
			}
		}
		// A hinted query must carry no status placeholders: INDEXED BY
		// a partial index is a prepare-time "no query solution" error
		// unless the WHERE clause provably implies the index predicate,
		// which bind parameters cannot satisfy — so the status set is
		// inlined as literals (see purgeVictimQuery). The unhinted query
		// keeps one placeholder per status.
		args := purgeVictimStatusArgs(c.sts)
		if c.want != "" {
			if len(args) != 0 {
				t.Errorf("%s: hinted victim status args = %v, want none (status set inlined)", c.name, args)
			}
			if strings.Contains(q, "? AND completed_at") || strings.Contains(q, "?,") {
				t.Errorf("%s: hinted victim query still binds statuses:\n%s", c.name, q)
			}
			// The inlined literals must match the forced index contents:
			// the default index predicate is only provable (and the scan
			// only correct) when the filter matches the index contents,
			// and likewise the continued index needs status = 'continued'.
			if c.want == "wf_instances_completed_at_idx" {
				for _, s := range backend.DefaultPurgeStatuses {
					if !strings.Contains(q, "'"+s+"'") {
						t.Errorf("%s: hinted victim query lost literal %q:\n%s", c.name, s, q)
					}
				}
			} else {
				if !strings.Contains(q, "'continued'") {
					t.Errorf("%s: continued victim query lost literal 'continued':\n%s", c.name, q)
				}
			}
		} else if len(args) != len(c.sts) {
			t.Errorf("%s: unhinted victim status args = %d, want %d (one per status)", c.name, len(args), len(c.sts))
		}
	}
}
