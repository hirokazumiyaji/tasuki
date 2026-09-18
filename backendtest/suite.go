package backendtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Factory constructs a fresh Backend for each subtest.
type Factory func(t *testing.T) backend.Backend

// ClockSetter is implemented by backends that support virtual time (memory).
type ClockSetter interface {
	SetNow(time.Time)
	Now() time.Time
}

// InboxSeqReader exposes the raw per-instance inbox sequence counter so the
// purge test can assert the counter is removed along with the instance.
type InboxSeqReader interface {
	InboxSeq(ctx context.Context, instanceID string) (int64, bool, error)
}

// Run executes the M1 conformance suite against a backend factory.
func Run(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("MigrateIdempotent", func(t *testing.T) { testMigrate(t, newBackend) })
	t.Run("CreateDuplicate", func(t *testing.T) { testCreateDuplicate(t, newBackend) })
	t.Run("LoadWorkflowHead", func(t *testing.T) { testLoadWorkflowHead(t, newBackend) })
	t.Run("CommitAdvancementConflict", func(t *testing.T) { testCommitConflict(t, newBackend) })
	t.Run("DoubleCompleteSuperseded", func(t *testing.T) { testDoubleComplete(t, newBackend) })
	t.Run("TerminateIgnoresLateComplete", func(t *testing.T) { testTerminateLateComplete(t, newBackend) })
	t.Run("FireTimerWakesWorkflow", func(t *testing.T) { testFireTimer(t, newBackend) })
	t.Run("CountClaimableTasks", func(t *testing.T) { testCountClaimableTasks(t, newBackend) })
	t.Run("SignalDedupe", func(t *testing.T) { testSignalDedupe(t, newBackend) })
	t.Run("SignalDedupeBatch", func(t *testing.T) { testSignalDedupeBatch(t, newBackend) })
	t.Run("InboxOrder", func(t *testing.T) { testInboxOrder(t, newBackend) })
	t.Run("NackTask", func(t *testing.T) { testNackTask(t, newBackend) })
	t.Run("SearchAttributes", func(t *testing.T) { testSearchAttributes(t, newBackend) })
	t.Run("Memo", func(t *testing.T) { testMemo(t, newBackend) })
	t.Run("PurgeInstances", func(t *testing.T) { testPurgeInstances(t, newBackend) })
	t.Run("TerminalLargeDedupe", func(t *testing.T) { testTerminalLargeDedupe(t, newBackend) })
	RunConcurrent(t, newBackend)
	RunM2(t, newBackend)
	RunM3(t, newBackend)
}

func testPurgeInstances(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	setNow(b, base)

	// Three terminal instances with distinct statuses plus one running.
	terminate := func(id, status, queue string) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		if status == "terminated" {
			if err := b.TerminateInstance(ctx, id); err != nil {
				t.Fatal(err)
			}
			return
		}
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "purger",
		})
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim %s: %v %#v", id, err, tasks)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		err = b.CommitAdvancement(ctx, backend.Advancement{
			InstanceID:  id,
			TaskID:      tasks[0].ID,
			ExpectedSeq: st.NextSeq,
			Terminal:    &backend.TerminalUpdate{Status: status},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	terminate("purge-done", "completed", "default")
	terminate("purge-err", "failed", "default")
	terminate("purge-stop", "terminated", "default")
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "purge-live", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Inbox/journal rows must disappear along with the instance row.
	if err := b.SendToInbox(ctx, "purge-done", journal.Event{Type: journal.TypeSignalReceived, Name: "late"}, ""); err != nil {
		t.Fatal(err)
	}
	if probe, ok := b.(InboxSeqReader); ok {
		if _, exists, err := probe.InboxSeq(ctx, "purge-done"); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("purge-done: inbox sequence counter missing before purge")
		}
	}

	// Non-terminal statuses are rejected outright.
	if _, err := b.PurgeInstances(ctx, time.Hour, []string{"running"}, 10); err == nil {
		t.Fatal("expected error for running status")
	}

	older := time.Hour
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(base.Add(2 * time.Hour))
	} else {
		older = 0 // real-time stores mark completions at wall-clock now
	}

	n, err := b.PurgeInstances(ctx, older, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("purged %d, want 3", n)
	}
	for _, id := range []string{"purge-done", "purge-err", "purge-stop"} {
		if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("%s: want ErrNotFound, got %v", id, err)
		}
	}
	// The per-instance inbox sequence counter must not survive the purge.
	if probe, ok := b.(InboxSeqReader); ok {
		if _, exists, err := probe.InboxSeq(ctx, "purge-done"); err != nil {
			t.Fatal(err)
		} else if exists {
			t.Fatal("purge-done: inbox sequence counter survived purge")
		}
	}
	list0, err := b.ListInstances(ctx, backend.InstanceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, inst := range list0 {
		for _, id := range []string{"purge-done", "purge-err", "purge-stop"} {
			if inst.ID == id {
				t.Fatalf("%s survived purge", id)
			}
		}
	}
	inst, err := b.GetInstance(ctx, "purge-live")
	if err != nil || inst.Status != "running" {
		t.Fatalf("running instance must survive purge: %v %#v", err, inst)
	}

	// Limit caps how many instances a single call removes.
	setNow(b, base)
	terminate("purge-a", "completed", "purge-q")
	terminate("purge-b", "completed", "purge-q")
	older = time.Hour
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(base.Add(4 * time.Hour))
	} else {
		older = 0
	}
	n, err = b.PurgeInstances(ctx, older, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("limited purge removed %d, want 1", n)
	}
	list, err := b.ListInstances(ctx, backend.InstanceFilter{Status: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("want one surviving instance, got %d", len(list))
	}
}

func testCountClaimableTasks(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "backlog-1", Name: "wf", Queue: "default", Input: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	counts, err := b.CountClaimableTasks(ctx, "workflow", []string{"default", "other"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["default"] < 1 {
		t.Fatalf("want default backlog >= 1, got %v", counts)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) < 1 {
		t.Fatal("expected to claim at least one task")
	}
	after, err := b.CountClaimableTasks(ctx, "workflow", []string{"default"})
	if err != nil {
		t.Fatal(err)
	}
	if after["default"] >= counts["default"] {
		t.Fatalf("backlog should drop after claim: before=%v after=%v", counts, after)
	}
}

func testLoadWorkflowHead(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "head-1", Name: "wf", Queue: "default", Input: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	full, err := b.LoadWorkflow(ctx, "head-1")
	if err != nil {
		t.Fatal(err)
	}
	head, err := b.LoadWorkflowHead(ctx, "head-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Journal) != 0 {
		t.Fatalf("head journal want empty, got %d", len(head.Journal))
	}
	if head.NextSeq != full.NextSeq || head.Instance.ID != full.Instance.ID || head.Instance.Status != full.Instance.Status {
		t.Fatalf("head meta mismatch: %+v vs %+v", head, full)
	}
	if len(head.Inbox) != len(full.Inbox) {
		t.Fatalf("inbox len %d vs %d", len(head.Inbox), len(full.Inbox))
	}
	if head.Now.IsZero() {
		t.Fatal("head Now is zero")
	}
}

func testMigrate(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
}

func testCreateDuplicate(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	inst := backend.NewInstance{ID: "dup-1", Name: "WF", Queue: "default"}
	if err := b.CreateInstance(ctx, inst); err != nil {
		t.Fatal(err)
	}
	err := b.CreateInstance(ctx, inst)
	if !errors.Is(err, backend.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}

func testCommitConflict(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "cas-1", Name: "WF", Queue: "default"})
	tasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	st, _ := b.LoadWorkflow(ctx, "cas-1")
	adv := backend.Advancement{
		InstanceID:  "cas-1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-02T00:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: st.NextSeq, FireAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}},
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatal(err)
	}
	// Wake via timer so we can claim again, then use stale ExpectedSeq.
	advanceTo(b, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if _, err := b.FireDueTimers(ctx, 10); err != nil {
		t.Fatal(err)
	}
	tasks2, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(tasks2) != 1 {
		t.Fatalf("claim2: %v %#v", err, tasks2)
	}
	st2, _ := b.LoadWorkflow(ctx, "cas-1")
	bad := backend.Advancement{
		InstanceID:  "cas-1",
		TaskID:      tasks2[0].ID,
		ExpectedSeq: st2.NextSeq - 1,
		NewEvents:   []journal.Event{{Seq: st2.NextSeq, Type: journal.TypeTimerFired, RefSeq: st.NextSeq}},
	}
	err = b.CommitAdvancement(ctx, bad)
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func testDoubleComplete(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	seq := scheduleActivity(t, b, "dbl-1", "greet")
	atasks, err := b.ClaimTasks(ctx, claimAct())
	if err != nil || len(atasks) != 1 {
		t.Fatalf("claim act: %v %#v", err, atasks)
	}
	ev := journal.Event{Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"ok"`)}
	if err := b.CompleteActivity(ctx, atasks[0].ID, ev); err != nil {
		t.Fatal(err)
	}
	err = b.CompleteActivity(ctx, atasks[0].ID, ev)
	if !errors.Is(err, backend.ErrSuperseded) {
		t.Fatalf("want ErrSuperseded, got %v", err)
	}
}

func testTerminateLateComplete(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	seq := scheduleActivity(t, b, "term-1", "greet")
	if err := b.TerminateInstance(ctx, "term-1"); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, "term-1")
	if err != nil || inst.Status != "terminated" {
		t.Fatalf("status: %v %#v", err, inst)
	}
	atasks, _ := b.ClaimTasks(ctx, claimAct())
	if len(atasks) != 0 {
		t.Fatalf("activity tasks should be gone, got %d", len(atasks))
	}
	err = b.CompleteActivity(ctx, 42, journal.Event{
		Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"late"`),
	})
	if !errors.Is(err, backend.ErrSuperseded) {
		t.Fatalf("want ErrSuperseded, got %v", err)
	}
	st, _ := b.LoadWorkflow(ctx, "term-1")
	if len(st.Inbox) != 0 {
		t.Fatalf("inbox not empty: %+v", st.Inbox)
	}
}

func testFireTimer(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "tm-1", Name: "WF", Queue: "default"})
	tasks, _ := b.ClaimTasks(ctx, claimWF())
	st, _ := b.LoadWorkflow(ctx, "tm-1")
	delay := time.Hour
	if _, ok := b.(ClockSetter); !ok {
		delay = 80 * time.Millisecond
	}
	fireAt := st.Now.Add(delay)
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "tm-1", TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
		Timers:    []backend.NewTimer{{Seq: st.NextSeq, FireAt: fireAt}},
	}); err != nil {
		t.Fatal(err)
	}
	advanceTo(b, fireAt)
	n, err := b.FireDueTimers(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("fire: n=%d err=%v", n, err)
	}
	wtasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(wtasks) != 1 {
		t.Fatalf("workflow should wake: %v %#v", err, wtasks)
	}
}

func scheduleActivity(t *testing.T, b backend.Backend, id, name string) int64 {
	t.Helper()
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim wf: %v %#v", err, tasks)
	}
	st, _ := b.LoadWorkflow(ctx, id)
	seq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: seq,
		NewEvents: []journal.Event{{Seq: seq, Type: journal.TypeActivityScheduled, Name: name}},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id, Name: name, Seq: seq, Input: []byte(`{}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	return seq
}

func claimWF() backend.ClaimRequest {
	return backend.ClaimRequest{Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "tester"}
}

// instanceID derives a per-test instance ID safe for document-keyed backends.
func instanceID(prefix string, t *testing.T) string {
	t.Helper()
	return prefix + strings.ReplaceAll(t.Name(), "/", "-")
}

func claimAct() backend.ClaimRequest {
	return backend.ClaimRequest{Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "tester"}
}

func setNow(b backend.Backend, now time.Time) {
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(now)
	}
}

func advanceTo(b backend.Backend, t time.Time) {
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(t)
		return
	}
	// Real-time backends: sleep until roughly t if in the near future.
	if d := time.Until(t); d > 0 && d < 5*time.Second {
		time.Sleep(d + 20*time.Millisecond)
	}
}
