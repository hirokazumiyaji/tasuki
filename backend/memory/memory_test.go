package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMemory_CreateAndLoadJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "i1", Name: "WF", Queue: "default", Input: []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if st.NextSeq != 2 {
		t.Fatalf("next_seq=%d want 2 (after workflow_started)", st.NextSeq)
	}
	if len(st.Journal) != 1 || st.Journal[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("journal=%+v", st.Journal)
	}
}

func TestMemory_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	inst := backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}
	if err := b.CreateInstance(ctx, inst); err != nil {
		t.Fatal(err)
	}
	err := b.CreateInstance(ctx, inst)
	if !errors.Is(err, backend.ErrAlreadyExists) {
		t.Fatalf("got %v", err)
	}
}

func TestMemory_CommitAdvancement_CAS(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	st, _ := b.LoadWorkflow(ctx, "i1")
	fireAt := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	adv := backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-08T00:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: st.NextSeq, FireAt: fireAt}},
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatal(err)
	}

	// Stale CAS with wrong expected seq
	tasks2, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks2) != 0 {
		t.Fatalf("no workflow task should exist after commit without inbox, got %d", len(tasks2))
	}

	// Force conflict: load current next_seq then commit with stale expected
	st2, _ := b.LoadWorkflow(ctx, "i1")
	// Re-create a workflow task by sending empty ensure via complete path:
	// Use FireDueTimers after advancing clock, then claim, then CAS fail.
	b.SetNow(fireAt)
	n, err := b.FireDueTimers(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("fire: n=%d err=%v", n, err)
	}
	tasks3, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks3) != 1 {
		t.Fatalf("claim after fire: %v %#v", err, tasks3)
	}
	bad := backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks3[0].ID,
		ExpectedSeq: st2.NextSeq - 1, // stale
		NewEvents:   []journal.Event{{Seq: st2.NextSeq, Type: journal.TypeTimerFired, RefSeq: st.NextSeq}},
	}
	err = b.CommitAdvancement(ctx, bad)
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestMemory_CompleteActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	tasks, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	st, _ := b.LoadWorkflow(ctx, "i1")
	seq := st.NextSeq
	err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: seq,
		NewEvents: []journal.Event{
			{Seq: seq, Type: journal.TypeActivityScheduled, Name: "greet"},
		},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: "i1",
			Name: "greet", Seq: seq, Input: []byte(`"x"`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	atasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(atasks) != 1 {
		t.Fatalf("activity claim: %v %#v", err, atasks)
	}
	err = b.CompleteActivity(ctx, atasks[0], journal.Event{
		Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"y"`),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Second complete should be superseded
	err = b.CompleteActivity(ctx, atasks[0], journal.Event{
		Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"z"`),
	})
	if !errors.Is(err, backend.ErrSuperseded) {
		t.Fatalf("want ErrSuperseded, got %v", err)
	}
	wtasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(wtasks) != 1 {
		t.Fatalf("workflow ensure: %v %#v", err, wtasks)
	}
}
