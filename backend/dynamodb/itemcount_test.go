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
		{
			// Terminal advancements skip activity/timer effects in
			// buildAdvancementItems, so the preflight must mirror the skip
			// (Codex round 9 on #328): counting ignored effects falsely
			// rejects small terminal batches as oversized.
			"terminal skips activities and timers",
			backend.Advancement{
				Terminal:      &backend.TerminalUpdate{Status: "completed"},
				ActivityTasks: []backend.NewTask{{}, {}},
				Timers:        []backend.NewTimer{{}},
			},
			false, 2,
		},
		{
			"terminal keeps journal/inbox/children/parent",
			backend.Advancement{
				Terminal:      &backend.TerminalUpdate{Status: "completed"},
				NewEvents:     []journal.Event{{Seq: 2}},
				ActivityTasks: []backend.NewTask{{}, {}},
				Timers:        []backend.NewTimer{{}},
				DrainedInbox:  []int64{7},
				Children:      []backend.NewInstance{{}},
				ParentNotify:  &notify,
			},
			true, 2 + 1 + 1 + 1*3 + 1,
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
