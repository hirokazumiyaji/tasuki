package tasuki

import (
	"context"
	"errors"
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

func TestLoadWorkflowState_StickyStaleRewind(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "rw1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	st, err := w.loadWorkflowState(ctx, "rw1")
	if err != nil {
		t.Fatal(err)
	}
	// Poison sticky cache to look ahead of store head → rewind path.
	w.setSticky("rw1", append([]journal.Event{}, st.Journal...), st.NextSeq+10)
	st2, err := w.loadWorkflowState(ctx, "rw1")
	if err != nil {
		t.Fatal(err)
	}
	if st2.NextSeq != st.NextSeq {
		t.Fatalf("want rewound next=%d got %d", st.NextSeq, st2.NextSeq)
	}
}

func TestCommitWorkflow_ConflictDropsSticky(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "cf1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	st, err := w.loadWorkflowState(ctx, "cf1")
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("%v", err)
	}
	err = w.commitWorkflow(ctx, tasks[0], st.Journal, backend.Advancement{
		InstanceID:  "cf1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq - 1, // stale
		NewEvents:   []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("got %v", err)
	}
	if _, ok := w.stickyGet("cf1"); ok {
		t.Fatal("sticky should be dropped on conflict")
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
			task:        tsk,
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

func TestLoadWorkflowState_StickyMergeMismatch(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "mm1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	st, err := w.loadWorkflowState(ctx, "mm1")
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("%v", err)
	}
	seq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "mm1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: seq,
		NewEvents: []journal.Event{
			{Seq: seq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-01T01:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: seq, FireAt: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}},
	}); err != nil {
		t.Fatal(err)
	}
	// Sticky lags head but ends with a bogus high seq → merge expectedNextSeq mismatches head.
	poison := append(append([]journal.Event{}, st.Journal...), journal.Event{Seq: 99, Type: journal.TypeTimerCreated})
	w.setSticky("mm1", poison, st.NextSeq)
	st2, err := w.loadWorkflowState(ctx, "mm1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st2.Journal) < 2 {
		t.Fatalf("want full reload journal, got %d", len(st2.Journal))
	}
	for _, e := range st2.Journal {
		if e.Seq == 99 {
			t.Fatal("poison event should not survive reload")
		}
	}
}

func TestFlushWorkflowCommits_BatchConflictDropsSticky(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	for _, id := range []string{"bc1", "bc2"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	// Production-like lease: without a lease release the conflicted task
	// would stay invisible for the full LeaseDuration.
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2, Lease: 30 * time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 2 {
		t.Fatalf("%v %#v", err, tasks)
	}
	byInst := map[string]backend.Task{}
	for _, tsk := range tasks {
		byInst[tsk.InstanceID] = tsk
	}
	pending := make([]pendingWorkflowCommit, 0, 2)
	for _, id := range []string{"bc1", "bc2"} {
		tsk := byInst[id]
		st, err := w.loadWorkflowState(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		exp := st.NextSeq
		if id == "bc2" {
			exp = 999 // force batch conflict on one instance
		}
		pending = append(pending, pendingWorkflowCommit{
			instanceID:  id,
			baseJournal: st.Journal,
			task:        tsk,
			adv: backend.Advancement{
				InstanceID:  id,
				TaskID:      tsk.ID,
				ExpectedSeq: exp,
				NewEvents: []journal.Event{
					{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-02T00:00:00Z"}`)},
				},
			},
		})
	}
	w.flushWorkflowCommits(ctx, pending)
	// Healthy instance advances in the same tick via per-instance fallback.
	st1, err := b.LoadWorkflow(ctx, "bc1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st1.Journal) < 2 {
		t.Fatalf("bc1 should advance despite bc2 conflict, journal=%d", len(st1.Journal))
	}
	if _, ok := w.stickyGet("bc1"); !ok {
		t.Fatal("bc1 sticky should be kept after successful fallback commit")
	}
	// Conflicted instance drops sticky and its task is immediately
	// reclaimable without waiting out the 30s lease.
	if _, ok := w.stickyGet("bc2"); ok {
		t.Fatal("bc2 sticky should be dropped after conflict")
	}
	reclaimed, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2, Lease: 30 * time.Second, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].InstanceID != "bc2" {
		t.Fatalf("want bc2 task immediately reclaimable, got %+v", reclaimed)
	}
}

func TestLoadWorkflowState_TerminalDropsSticky(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "term1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	if _, err := w.loadWorkflowState(ctx, "term1"); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, "term1"); err != nil {
		t.Fatal(err)
	}
	st, err := w.loadWorkflowState(ctx, "term1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Instance.Status == "running" {
		t.Fatal("want terminal")
	}
	if _, ok := w.stickyGet("term1"); ok {
		t.Fatal("sticky dropped for non-running")
	}
}
