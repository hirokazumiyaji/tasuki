package worker

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
	if got := advancementOps(adv, backend.Capabilities{}); got != 14 {
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
	// are discarded by the backend commit and must not count. DynamoDB
	// deletes each drained inbox row inside the terminal transaction, so
	// the in-txn caps below keep the per-row charge (conservative path).
	inTxnCaps := backend.Capabilities{MaxAdvancementEffects: 80}
	if got := advancementOps(adv, inTxnCaps); got != 42 {
		t.Fatalf("terminal advancementOps = %d, want 42 (activities/timers excluded)", got)
	}
	// Same advancement without the terminal flag counts everything:
	// 2 + 39 + 39 + 3 + 1 = 84.
	adv.Terminal = nil
	if got := advancementOps(adv, inTxnCaps); got != 84 {
		t.Fatalf("suspended advancementOps = %d, want 84", got)
	}
	// A terminal turn heavy with discarded effects still fits the budget.
	w := NewWorker(memory.New(), WorkerOptions{})
	adv.Terminal = term
	if err := w.checkTerminalBudget(adv); err != nil {
		t.Fatalf("terminal with discarded effects should fit: %v", err)
	}
}

// sweepBackend wraps memory with explicit capabilities (Firestore-like
// bounded budget with post-commit terminal inbox sweep, or DynamoDB-like
// in-transaction inbox deletes).
type sweepBackend struct {
	*memory.Backend
	caps backend.Capabilities
}

func (b *sweepBackend) Capabilities() backend.Capabilities { return b.caps }

// Terminal turns on backends that sweep the inbox post-commit must not
// charge one op per DrainedInbox row (Codex round 14 on #328): Firestore
// drains its normal 190-row maximum, emits 18 commands plus the terminal
// event, and sweeps the inbox after committing, so the commit costs ~211
// writes — charging 190 deletes on top (401) falsely rejects a completable
// turn against the 400 budget.
func TestAdvancementOps_TerminalSweepExcludesInboxDeletes(t *testing.T) {
	term := &backend.TerminalUpdate{Status: "completed"}
	drained := make([]int64, 190)
	for i := range drained {
		drained[i] = int64(i + 1)
	}
	adv := &backend.Advancement{
		// 190 ingested signals + 18 commands + 1 terminal event.
		NewEvents:    make([]journal.Event, 209),
		DrainedInbox: drained,
		Terminal:     term,
	}
	// Sweeping backend: 2 + 209 journal, no per-row inbox charge = 211.
	sweepCaps := backend.Capabilities{MaxAdvancementEffects: 400, SweepsTerminalInbox: true}
	if got := advancementOps(adv, sweepCaps); got != 211 {
		t.Fatalf("sweeping terminal advancementOps = %d, want 211 (inbox sweep excluded)", got)
	}
	// In-transaction backend (DynamoDB): 2 + 209 + 190 = 401.
	inTxnCaps := backend.Capabilities{MaxAdvancementEffects: 400}
	if got := advancementOps(adv, inTxnCaps); got != 401 {
		t.Fatalf("in-txn terminal advancementOps = %d, want 401 (inbox deletes counted)", got)
	}
	// The sweeping terminal turn fits its budget; the in-txn one does not.
	w := NewWorker(&sweepBackend{Backend: memory.New(), caps: sweepCaps}, WorkerOptions{})
	if err := w.checkTerminalBudget(adv); err != nil {
		t.Fatalf("sweeping terminal should fit 400 budget: %v", err)
	}
	w2 := NewWorker(&sweepBackend{Backend: memory.New(), caps: inTxnCaps}, WorkerOptions{})
	if err := w2.checkTerminalBudget(adv); err == nil {
		t.Fatal("in-txn terminal draining 190 rows should exceed 400 budget")
	}
}
