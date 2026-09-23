package dynamodb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestCommitAdvancements_BatchOK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := newBackend(t)
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
	}

	for _, id := range []string{"batch-a", "batch-b"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 2 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	advs := make([]backend.Advancement, 0, 2)
	for _, tsk := range tasks {
		st, err := b.LoadWorkflow(ctx, tsk.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		advs = append(advs, backend.Advancement{
			InstanceID:  tsk.InstanceID,
			TaskID:      tsk.ID,
			ExpectedSeq: st.NextSeq,
			NewEvents: []journal.Event{{
				Seq: st.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`),
			}},
			Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
		})
	}
	if err := b.CommitAdvancements(ctx, advs); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"batch-a", "batch-b"} {
		inst, err := b.GetInstance(ctx, id)
		if err != nil || inst.Status != "completed" {
			t.Fatalf("%s status=%v err=%v", id, inst, err)
		}
	}
}

func TestCommitAdvancements_ConflictRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := newBackend(t)
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
	}

	for _, id := range []string{"rb-a", "rb-b"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 2 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	byID := map[string]backend.Task{}
	for _, tsk := range tasks {
		byID[tsk.InstanceID] = tsk
	}
	stA, _ := b.LoadWorkflow(ctx, "rb-a")
	stB, _ := b.LoadWorkflow(ctx, "rb-b")
	good := backend.Advancement{
		InstanceID: "rb-a", TaskID: byID["rb-a"].ID, ExpectedSeq: stA.NextSeq,
		NewEvents: []journal.Event{{Seq: stA.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`)}},
		Terminal:  &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}
	bad := backend.Advancement{
		InstanceID: "rb-b", TaskID: byID["rb-b"].ID, ExpectedSeq: stB.NextSeq - 1,
		NewEvents: []journal.Event{{Seq: stB.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`)}},
		Terminal:  &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}
	err = b.CommitAdvancements(ctx, []backend.Advancement{good, bad})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	instA, _ := b.GetInstance(ctx, "rb-a")
	instB, _ := b.GetInstance(ctx, "rb-b")
	if instA.Status != "running" || instB.Status != "running" {
		t.Fatalf("want both running after rollback; a=%s b=%s", instA.Status, instB.Status)
	}
}

// An oversized combined batch of distinct instances must be rejected BEFORE
// applying anything (Codex round 6 on #330): the 100-item TransactWriteItems
// limit cannot hold the batch atomically, and the old sequential fallback
// committed it partially (applied the good advancements, then ErrConflict on
// a stale one). The rejection is a sizing error, never ErrConflict, and every
// instance stays untouched.
func TestCommitAdvancements_OversizedBatchRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := newBackend(t)
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
	}

	for _, id := range []string{"over-a", "over-b"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 2 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	var advs []backend.Advancement
	for _, tsk := range tasks {
		st, err := b.LoadWorkflow(ctx, tsk.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		events := make([]journal.Event, 0, 60)
		for i := 0; i < 60; i++ {
			events = append(events, journal.Event{
				Seq: st.NextSeq + int64(i), Type: journal.TypeSignalReceived, Name: "pad",
			})
		}
		advs = append(advs, backend.Advancement{
			InstanceID: tsk.InstanceID, TaskID: tsk.ID, ExpectedSeq: st.NextSeq, NewEvents: events,
		})
	}
	// 2 + 60 journal puts per advancement = 62 ops each, 124 combined.
	err = b.CommitAdvancements(ctx, advs)
	if err == nil {
		t.Fatal("oversized batch: want sizing error, got nil")
	}
	if errors.Is(err, backend.ErrConflict) {
		t.Fatalf("oversized batch: want sizing error, got ErrConflict (%v)", err)
	}
	for _, id := range []string{"over-a", "over-b"} {
		inst, err := b.GetInstance(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if inst.Status != "running" || inst.NextSeq != 2 {
			t.Fatalf("oversized batch partially applied: %s status=%s nextSeq=%d", id, inst.Status, inst.NextSeq)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Journal) != 1 {
			t.Fatalf("oversized batch partially applied: %s journal=%d want 1", id, len(st.Journal))
		}
	}
}
