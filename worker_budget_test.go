package tasuki

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// advancementOps must mirror backend/dynamodb buildAdvancementItems counting:
// 2 (instance CAS + task delete) + journal puts + activity/timer puts +
// inbox deletes + 3 per child + parent notify.
func TestAdvancementOps_CountsAllEffects(t *testing.T) {
	adv := &backend.Advancement{
		NewEvents:     make([]journal.Event, 3),
		ActivityTasks: make([]backend.NewTask, 2),
		Timers:        make([]backend.NewTimer, 1),
		DrainedInbox:  []int64{7, 8},
		Children:      make([]backend.NewInstance, 1),
		ParentNotify:  &journal.Event{Type: journal.TypeChildCompleted},
	}
	// 2 + 3 + 2 + 1 + 2 + 3 + 1 = 14.
	if got := advancementOps(adv); got != 14 {
		t.Fatalf("got %d want 14", got)
	}
}

// Terminal advancements that cannot fit the budget must fail fast with a
// diagnostic instead of hitting the backend transaction limit.
func TestCheckTerminalBudget(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{})
	// Memory backend is unlimited (budget 100): small advancement passes.
	small := &backend.Advancement{
		NewEvents:    make([]journal.Event, 2),
		DrainedInbox: []int64{1},
		Terminal:     &backend.TerminalUpdate{Status: "completed"},
	}
	if err := w.checkTerminalBudget(small); err != nil {
		t.Fatalf("small terminal should fit: %v", err)
	}
	// 2 + 90 journal + terminal overhead exceeds the 100 budget.
	big := &backend.Advancement{
		NewEvents:     make([]journal.Event, 90),
		ActivityTasks: make([]backend.NewTask, 90),
		Terminal:      &backend.TerminalUpdate{Status: "completed"},
	}
	if err := w.checkTerminalBudget(big); err == nil {
		t.Fatal("oversized terminal should be rejected with a diagnostic")
	}
}
