package backendtest

import (
	"context"
	"errors"
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

// Run executes the M1 conformance suite against a backend factory.
func Run(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("MigrateIdempotent", func(t *testing.T) { testMigrate(t, newBackend) })
	t.Run("CreateDuplicate", func(t *testing.T) { testCreateDuplicate(t, newBackend) })
	t.Run("CommitAdvancementConflict", func(t *testing.T) { testCommitConflict(t, newBackend) })
	t.Run("DoubleCompleteSuperseded", func(t *testing.T) { testDoubleComplete(t, newBackend) })
	t.Run("TerminateIgnoresLateComplete", func(t *testing.T) { testTerminateLateComplete(t, newBackend) })
	t.Run("FireTimerWakesWorkflow", func(t *testing.T) { testFireTimer(t, newBackend) })
	RunConcurrent(t, newBackend)
	RunM2(t, newBackend)
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
