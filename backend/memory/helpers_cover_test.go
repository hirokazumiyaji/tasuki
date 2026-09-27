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

func TestMemory_RecordHeartbeatAndTimerHelpers(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)

	if _, ok := b.NextTimerFireAt(); ok {
		t.Fatal("empty timers")
	}
	if b.HasRunnableTasks() {
		t.Fatal("no tasks yet")
	}

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if !b.HasRunnableTasks() {
		t.Fatal("want runnable after create")
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}

	if err := b.RecordHeartbeat(ctx, tasks[0], 5*time.Second, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.RecordHeartbeat(ctx, backend.Task{ID: 99999}, time.Second, nil); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	st, _ := b.LoadWorkflow(ctx, "i1")
	fireAt := now.Add(24 * time.Hour)
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-02T00:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: st.NextSeq, FireAt: fireAt}},
	}); err != nil {
		t.Fatal(err)
	}

	next, ok := b.NextTimerFireAt()
	if !ok || !next.Equal(fireAt) {
		t.Fatalf("next=%v ok=%v want %v", next, ok, fireAt)
	}
}
