package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// testTerminalCleanup verifies that a terminal CommitAdvancement removes
// pending activity tasks, timers, and inbox rows in the same transaction,
// and that FireDueTimers never resurrects inbox rows for terminal instances
// (issue #291).
func testTerminalCleanup(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	setNow(b, base)

	const id = "term-cleanup-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim wf: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	delay := time.Hour
	if _, ok := b.(ClockSetter); !ok {
		delay = 200 * time.Millisecond
	}
	fireAt := st.Now.Add(delay)
	actSeq := st.NextSeq
	tmrSeq := st.NextSeq + 1
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  id,
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: actSeq, Type: journal.TypeActivityScheduled, Name: "work"},
			{Seq: tmrSeq, Type: journal.TypeTimerCreated},
		},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id, Name: "work", Seq: actSeq, Input: []byte(`{}`),
		}},
		Timers:             []backend.NewTimer{{Seq: tmrSeq, FireAt: fireAt}},
		EnsureWorkflowTask: true,
	}); err != nil {
		t.Fatal(err)
	}

	counts, err := b.CountClaimableTasks(ctx, "activity", []string{"default"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["default"] < 1 {
		t.Fatalf("want pending activity before terminal, got %v", counts)
	}

	wtasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(wtasks) != 1 {
		t.Fatalf("claim wf for terminal: %v %#v", err, wtasks)
	}
	st2, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Terminal advancement also carries scheduling garbage: a correct
	// backend must not leave it behind.
	garbageActSeq := st2.NextSeq + 1
	garbageTmrSeq := st2.NextSeq + 2
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  id,
		TaskID:      wtasks[0].ID,
		ExpectedSeq: st2.NextSeq,
		NewEvents: []journal.Event{
			{Seq: st2.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`)},
		},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id, Name: "garbage", Seq: garbageActSeq, Input: []byte(`{}`),
		}},
		Timers:   []backend.NewTimer{{Seq: garbageTmrSeq, FireAt: fireAt}},
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}); err != nil {
		t.Fatal(err)
	}

	inst, err := b.GetInstance(ctx, id)
	if err != nil || inst.Status != "completed" {
		t.Fatalf("status: %v %#v", err, inst)
	}
	atasks, err := b.ClaimTasks(ctx, claimAct())
	if err != nil {
		t.Fatal(err)
	}
	if len(atasks) != 0 {
		t.Fatalf("activity tasks should be gone after terminal, got %d", len(atasks))
	}
	st3, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st3.Inbox) != 0 {
		t.Fatalf("inbox should be empty after terminal, got %+v", st3.Inbox)
	}

	advanceTo(b, fireAt.Add(time.Millisecond))
	n, err := b.FireDueTimers(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("FireDueTimers on terminal should fire 0, got %d", n)
	}
	st4, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st4.Inbox) != 0 {
		t.Fatalf("FireDueTimers must not create inbox for terminal, got %+v", st4.Inbox)
	}
	wtasks2, err := b.ClaimTasks(ctx, claimWF())
	if err != nil {
		t.Fatal(err)
	}
	for _, wt := range wtasks2 {
		if wt.InstanceID == id {
			t.Fatalf("workflow task should be gone after terminal, got %+v", wt)
		}
	}
}
