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
	// 2 + 99 journal exceeds the 100 budget even without any activities.
	big := &backend.Advancement{
		NewEvents: make([]journal.Event, 99),
		Terminal:  &backend.TerminalUpdate{Status: "completed"},
	}
	if err := w.checkTerminalBudget(big); err == nil {
		t.Fatal("oversized terminal should be rejected with a diagnostic")
	}
}

// Terminal advancements skip activity and timer effects in the backend
// (backend/dynamodb buildAdvancementItems breaks out of both loops when
// Terminal != nil), so the worker estimate must mirror the skip (Codex round
// 13 on #328): counting discarded effects falsely rejects completable
// terminal turns (39 activities + terminal event estimated 81 ops vs the real
// 42, over the 80-op budget).
func TestAdvancementOps_TerminalExcludesDiscardedEffects(t *testing.T) {
	term := &backend.TerminalUpdate{Status: "completed"}
	adv := &backend.Advancement{
		NewEvents:     make([]journal.Event, 39),
		ActivityTasks: make([]backend.NewTask, 39),
		Timers:        make([]backend.NewTimer, 3),
		DrainedInbox:  []int64{9},
		Terminal:      term,
	}
	// 2 + 39 journal + 1 inbox delete = 42; the 39 activities and 3 timers
	// are discarded by the backend commit and must not count.
	if got := advancementOps(adv); got != 42 {
		t.Fatalf("terminal advancementOps = %d, want 42 (activities/timers excluded)", got)
	}
	// Same advancement without the terminal flag counts everything:
	// 2 + 39 + 39 + 3 + 1 = 84.
	adv.Terminal = nil
	if got := advancementOps(adv); got != 84 {
		t.Fatalf("suspended advancementOps = %d, want 84", got)
	}
	// A terminal turn heavy with discarded effects still fits the budget.
	w := NewWorker(memory.New(), WorkerOptions{})
	adv.Terminal = term
	if err := w.checkTerminalBudget(adv); err != nil {
		t.Fatalf("terminal with discarded effects should fit: %v", err)
	}
}
