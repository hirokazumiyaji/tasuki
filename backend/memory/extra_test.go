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

func TestMemory_GetJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	events, err := b.GetJournal(ctx, "i1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("got %+v", events)
	}
	events, err = b.GetJournal(ctx, "i1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("afterSeq=1 should be empty, got %+v", events)
	}
}

func TestMemory_TerminateIgnoresLateComplete(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	tasks, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	st, _ := b.LoadWorkflow(ctx, "i1")
	seq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "i1", TaskID: tasks[0].ID, ExpectedSeq: seq,
		NewEvents: []journal.Event{{Seq: seq, Type: journal.TypeActivityScheduled, Name: "greet"}},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: "i1", Name: "greet", Seq: seq, Input: []byte(`"x"`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	inst, _ := b.GetInstance(ctx, "i1")
	if inst.Status != "terminated" {
		t.Fatalf("status=%s", inst.Status)
	}
	atasks, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if len(atasks) != 0 {
		t.Fatalf("activity tasks should be removed on terminate, got %d", len(atasks))
	}
	// Late complete on a stale id
	err := b.CompleteActivity(ctx, backend.Task{ID: 9999, Kind: "activity", WorkerID: "missing", Attempt: 1}, journal.Event{Type: journal.TypeActivityCompleted, Payload: []byte(`"y"`)})
	if !errors.Is(err, backend.ErrSuperseded) {
		t.Fatalf("want ErrSuperseded, got %v", err)
	}
	st2, _ := b.LoadWorkflow(ctx, "i1")
	if len(st2.Inbox) != 0 {
		t.Fatalf("inbox should stay empty, got %+v", st2.Inbox)
	}
}

func TestMemory_ExtendLeaseAndRetryActivity(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	wtasks, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	st, _ := b.LoadWorkflow(ctx, "i1")
	seq := st.NextSeq
	_ = b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "i1", TaskID: wtasks[0].ID, ExpectedSeq: seq,
		NewEvents: []journal.Event{{Seq: seq, Type: journal.TypeActivityScheduled, Name: "greet"}},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: "i1", Name: "greet", Seq: seq,
		}},
	})
	atasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(atasks) != 1 {
		t.Fatalf("claim activity: %v %#v", err, atasks)
	}
	if err := b.ExtendLease(ctx, atasks[0], 30*time.Second); err != nil {
		t.Fatal(err)
	}
	// Task not visible until lease expires
	b.SetNow(now.Add(5 * time.Second))
	again, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	if len(again) != 0 {
		t.Fatal("should still be leased")
	}
	const backoff = time.Minute
	if err := b.RetryActivity(ctx, atasks[0], backoff); err != nil {
		t.Fatal(err)
	}
	// visible_at = store now (now+5s) + backoff
	b.SetNow(now.Add(30 * time.Second))
	again, _ = b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	if len(again) != 0 {
		t.Fatal("should be waiting on backoff")
	}
	b.SetNow(now.Add(5*time.Second + backoff))
	again, _ = b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	if len(again) != 1 {
		t.Fatalf("want claim after backoff, got %d", len(again))
	}
}

func TestMemory_ReleaseLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	tasks, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err := b.ReleaseLease(ctx, tasks[0]); err != nil {
		t.Fatal(err)
	}
	// Another worker can claim immediately
	tasks2, _ := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	if len(tasks2) != 1 {
		t.Fatalf("want re-claim after release, got %d", len(tasks2))
	}
}

func TestMemory_MigrateNoop(t *testing.T) {
	b := memory.New()
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMemory_ListInstances(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "a", Name: "WF", Queue: "default"})
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "b", Name: "Other", Queue: "default"})
	_ = b.TerminateInstance(ctx, "b")
	all, err := b.ListInstances(ctx, backend.InstanceFilter{})
	if err != nil || len(all) != 2 {
		t.Fatalf("all: %v %#v", err, all)
	}
	term, err := b.ListInstances(ctx, backend.InstanceFilter{Status: "terminated"})
	if err != nil || len(term) != 1 || term[0].ID != "b" {
		t.Fatalf("terminated: %v %#v", err, term)
	}
	named, err := b.ListInstances(ctx, backend.InstanceFilter{Name: "WF"})
	if err != nil || len(named) != 1 || named[0].ID != "a" {
		t.Fatalf("name: %v %#v", err, named)
	}
}
