package dynamodb

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// advancementItemCount must stay in lockstep with buildAdvancementItems: the
// combined-batch preflight in CommitAdvancements rejects oversized batches on
// this count before building anything, and the defensive in-loop guard
// compares it against the real TransactWriteItems length.
func TestAdvancementItemCount(t *testing.T) {
	notify := journal.Event{Type: journal.TypeChildCompleted}
	cases := []struct {
		name      string
		adv       backend.Advancement
		hasParent bool
		want      int
	}{
		{"minimal", backend.Advancement{}, false, 2},
		{
			"journal only",
			backend.Advancement{NewEvents: []journal.Event{{Seq: 2}, {Seq: 3}, {Seq: 4}}},
			false, 2 + 3,
		},
		{
			"full house",
			backend.Advancement{
				NewEvents:     []journal.Event{{Seq: 2}},
				ActivityTasks: []backend.NewTask{{}, {}},
				Timers:        []backend.NewTimer{{}},
				DrainedInbox:  []int64{7, 8},
				Children:      []backend.NewInstance{{}, {}},
				ParentNotify:  &notify,
			},
			true, 2 + 1 + 2 + 1 + 2 + 2*3 + 1,
		},
		{
			"parent notify without parent adds nothing",
			backend.Advancement{ParentNotify: &notify},
			false, 2,
		},
		{
			"ensure task is one op either way",
			backend.Advancement{EnsureWorkflowTask: true},
			false, 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := advancementItemCount(tc.adv, tc.hasParent); got != tc.want {
				t.Fatalf("advancementItemCount = %d, want %d", got, tc.want)
			}
		})
	}
}
