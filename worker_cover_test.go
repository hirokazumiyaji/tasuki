package tasuki

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/observability"
)

func TestSampleBacklog_RecordsMetrics(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "b1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{
		Queues:  []string{"default"},
		Metrics: observability.MustNewMetrics(),
	})
	w.sampleBacklog(ctx) // should not panic; hits CountClaimableTasks + RecordBacklog
}

func TestLoadWorkflowState_StickyHitAndMerge(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "s1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})

	st1, err := w.loadWorkflowState(ctx, "s1")
	if err != nil || len(st1.Journal) == 0 {
		t.Fatalf("%v %+v", err, st1)
	}
	// Second load with unchanged next_seq uses sticky cache.
	st2, err := w.loadWorkflowState(ctx, "s1")
	if err != nil || len(st2.Journal) != len(st1.Journal) {
		t.Fatalf("%v len=%d", err, len(st2.Journal))
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v", err)
	}
	fireAt := now.Add(time.Hour)
	seq := st1.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "s1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: seq,
		NewEvents: []journal.Event{
			{Seq: seq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-01T01:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: seq, FireAt: fireAt}},
	}); err != nil {
		t.Fatal(err)
	}
	// head.NextSeq > sticky → merge path (expectedNextSeq)
	st3, err := w.loadWorkflowState(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st3.Journal) < len(st1.Journal)+1 {
		t.Fatalf("want merged journal, got %d", len(st3.Journal))
	}
}

func TestWithParentNotify_Failed(t *testing.T) {
	w := &Worker{}
	adv := &backend.Advancement{
		Terminal: &backend.TerminalUpdate{Status: "failed"},
	}
	w.withParentNotify(adv, "parent", 9)
	if adv.ParentNotify == nil || adv.ParentNotify.Type != journal.TypeChildFailed || adv.ParentNotify.RefSeq != 9 {
		t.Fatalf("%+v", adv.ParentNotify)
	}
}

func TestFlushWorkflowCommits_Batch(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	w.flushWorkflowCommits(ctx, nil) // empty no-op

	for _, id := range []string{"f1", "f2"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 2 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	pending := make([]pendingWorkflowCommit, 0, 2)
	for _, tsk := range tasks {
		st, err := b.LoadWorkflow(ctx, tsk.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		pending = append(pending, pendingWorkflowCommit{
			instanceID:  tsk.InstanceID,
			baseJournal: st.Journal,
			adv: backend.Advancement{
				InstanceID:  tsk.InstanceID,
				TaskID:      tsk.ID,
				ExpectedSeq: st.NextSeq,
				NewEvents: []journal.Event{
					{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-02T00:00:00Z"}`)},
				},
				Timers: []backend.NewTimer{{
					Seq: st.NextSeq, FireAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
				}},
			},
		})
	}
	w.flushWorkflowCommits(ctx, pending)
	for _, id := range []string{"f1", "f2"} {
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Journal) < 2 {
			t.Fatalf("%s journal=%d", id, len(st.Journal))
		}
	}
}
