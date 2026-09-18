package backendtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Terminal-transition cleanup (#299 item 1, see #290).
//
// Once an instance leaves "running" — via a terminal CommitAdvancement or
// TerminateInstance — no activity task may remain claimable and FireDueTimers
// must not create inbox rows or workflow tasks for it. Backends that historically
// left tasks/timers/inbox behind fail here (SQL backends did before the fix).
func testTerminalCleanup(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("CommitTerminal", func(t *testing.T) {
		terminalCleanupCase(t, newBackend, true)
	})
	t.Run("Terminate", func(t *testing.T) {
		terminalCleanupCase(t, newBackend, false)
	})
}

func terminalCleanupCase(t *testing.T, newBackend Factory, viaCommit bool) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	requireTerminalCleanup(t, b)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	setNow(b, base)
	id := instanceID("term-clean-", t)

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "term-clean",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim wf: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	actSeq, timerSeq := st.NextSeq, st.NextSeq+1
	delay := time.Hour
	if _, ok := b.(ClockSetter); !ok {
		delay = 80 * time.Millisecond
	}
	fireAt := st.Now.Add(delay)
	adv := backend.Advancement{
		InstanceID:  id,
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: actSeq, Type: journal.TypeActivityScheduled, Name: "step"},
			{Seq: timerSeq, Type: journal.TypeTimerCreated},
		},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id,
			Name: "step", Seq: actSeq, Input: []byte(`{}`),
		}},
		Timers: []backend.NewTimer{{Seq: timerSeq, FireAt: fireAt}},
	}
	if viaCommit {
		adv.Terminal = &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)}
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatal(err)
	}
	if !viaCommit {
		if err := b.TerminateInstance(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	assertNoRemnants(t, b, id, fireAt)
}

// assertNoRemnants checks the terminal invariant: no claimable tasks for the
// instance and no inbox growth from FireDueTimers.
func assertNoRemnants(t *testing.T, b backend.Backend, id string, fireAt time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, kind := range []string{"activity", "workflow"} {
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: kind, Queues: []string{"default"}, Limit: 100,
			Lease: time.Minute, WorkerID: "term-check",
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range tasks {
			if task.InstanceID == id {
				t.Fatalf("%s task %d survived terminal transition of %s", kind, task.ID, id)
			}
		}
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	inboxBefore := len(st.Inbox)
	advanceTo(b, fireAt.Add(time.Second))
	if _, err := b.FireDueTimers(ctx, 100); err != nil {
		t.Fatal(err)
	}
	st, err = b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != inboxBefore {
		t.Fatalf("FireDueTimers grew inbox of terminal %s: %d -> %d", id, inboxBefore, len(st.Inbox))
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 100,
		Lease: time.Minute, WorkerID: "term-check",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.InstanceID == id {
			t.Fatalf("FireDueTimers woke terminal %s (task %d)", id, task.ID)
		}
	}
}

// CompleteActivity vs TerminateInstance race (#299 item 2, see #291).
//
// Whichever wins, a subsequent TerminateInstance must leave no inbox rows and
// no claimable tasks behind. The race itself must not error unexpectedly:
// losers observe ErrSuperseded (or a benign nil when the task is already gone).
func testTerminateCompleteRace(t *testing.T, newBackend Factory) {
	t.Helper()
	const rounds = 15
	for r := 0; r < rounds; r++ {
		func() {
			ctx := context.Background()
			probe := newBackend(t)
			strict := probe.Capabilities().CleansTerminalState
			b := newBackend(t)
			setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			id := fmt.Sprintf("term-race-%d", r)
			seq := scheduleActivity(t, b, id, "act")
			atasks, err := b.ClaimTasks(ctx, claimAct())
			if err != nil || len(atasks) != 1 {
				t.Fatalf("round %d: claim act: %v %#v", r, err, atasks)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			var completeErr, terminateErr error
			go func() {
				defer wg.Done()
				completeErr = b.CompleteActivity(ctx, atasks[0].ID, journal.Event{
					Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"ok"`),
				})
			}()
			go func() {
				defer wg.Done()
				terminateErr = b.TerminateInstance(ctx, id)
			}()
			wg.Wait()
			if terminateErr != nil {
				t.Fatalf("round %d: TerminateInstance: %v", r, terminateErr)
			}
			if completeErr != nil && !isBenignRaceErr(completeErr) {
				t.Fatalf("round %d: CompleteActivity: %v", r, completeErr)
			}
			// Force the terminal end state, then require it to be clean.
			if err := b.TerminateInstance(ctx, id); err != nil {
				t.Fatalf("round %d: second TerminateInstance: %v", r, err)
			}
			inst, err := b.GetInstance(ctx, id)
			if err != nil || inst.Status != "terminated" {
				t.Fatalf("round %d: status: %v %#v", r, err, inst)
			}
			if !strict {
				t.Skip("terminal cleanup not implemented (see #291)")
			}
			st, err := b.LoadWorkflow(ctx, id)
			if err != nil {
				t.Fatalf("round %d: load: %v", r, err)
			}
			if len(st.Inbox) != 0 {
				t.Fatalf("round %d: terminated %s has %d inbox rows", r, id, len(st.Inbox))
			}
			for _, kind := range []string{"activity", "workflow"} {
				tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
					Kind: kind, Queues: []string{"default"}, Limit: 100,
					Lease: time.Minute, WorkerID: "race-check",
				})
				if err != nil {
					t.Fatalf("round %d: claim %s: %v", r, kind, err)
				}
				for _, task := range tasks {
					if task.InstanceID == id {
						t.Fatalf("round %d: %s task %d survived terminate of %s", r, kind, task.ID, id)
					}
				}
			}
		}()
	}
}

func isBenignRaceErr(err error) bool {
	return err == nil ||
		errors.Is(err, backend.ErrSuperseded) ||
		errors.Is(err, backend.ErrNotFound) ||
		errors.Is(err, backend.ErrConflict)
}

func requireTerminalCleanup(t *testing.T, b backend.Backend) {
	t.Helper()
	if !b.Capabilities().CleansTerminalState {
		t.Skip("terminal cleanup not implemented by this backend (see #290)")
	}
}
