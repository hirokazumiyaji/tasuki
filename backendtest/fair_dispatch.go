package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// RunFairDispatch exercises ClaimRequest.MaxPerInstance on backends that
// implement fair dispatch (postgres, mysql, sqlite, memory).
//
// The scenario is head-of-line blocking: instance "fair-flood" enqueues five
// activity tasks before two single-task instances. A capped batch must
// interleave the victims; the uncapped batch stays strict FIFO.
func RunFairDispatch(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))

	const queue = "fq"
	spawn := func(id string, activities int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "fair",
		})
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim wf %s: %v %#v", id, err, tasks)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		adv := backend.Advancement{
			InstanceID:  id,
			TaskID:      tasks[0].ID,
			ExpectedSeq: st.NextSeq,
		}
		for i := 0; i < activities; i++ {
			seq := st.NextSeq + int64(i)
			adv.NewEvents = append(adv.NewEvents, journal.Event{
				Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
			})
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind: "activity", Queue: queue, InstanceID: id, Name: "step",
				Seq: seq, Input: []byte(`{}`),
			})
		}
		if err := b.CommitAdvancement(ctx, adv); err != nil {
			t.Fatal(err)
		}
	}
	spawn("fair-flood", 7)
	spawn("fair-va", 1)
	spawn("fair-vb", 1)

	claim := func(limit, cap int) []backend.Task {
		t.Helper()
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "activity", Queues: []string{queue}, Limit: limit,
			Lease: time.Minute, WorkerID: "fair", MaxPerInstance: cap,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tasks
	}
	instances := func(tasks []backend.Task) []string {
		out := make([]string, 0, len(tasks))
		for _, task := range tasks {
			out = append(out, task.InstanceID)
		}
		return out
	}

	// Strict FIFO would fill the batch with fair-flood tasks. With cap=1 the
	// same batch must interleave both victims.
	got := claim(3, 1)
	if len(got) != 3 {
		t.Fatalf("cap1: want 3 tasks, got %v", instances(got))
	}
	seen := map[string]bool{}
	for _, id := range instances(got) {
		seen[id] = true
	}
	for _, want := range []string{"fair-flood", "fair-va", "fair-vb"} {
		if !seen[want] {
			t.Fatalf("cap1: %v missing %s", got, want)
		}
	}

	// Without the cap the flood instance alone fills the batch.
	got = claim(4, 0)
	if len(got) != 4 {
		t.Fatalf("uncapped: want 4 flood tasks, got %v", instances(got))
	}
	for _, task := range got {
		if task.InstanceID != "fair-flood" {
			t.Fatalf("uncapped: expected only flood tasks, got %v", instances(got))
		}
	}

	// A wider cap lets the flood instance take the rest, at most two at once.
	got = claim(10, 2)
	if len(got) != 2 {
		t.Fatalf("cap2: want 2 tasks, got %v", instances(got))
	}
	for _, task := range got {
		if task.InstanceID != "fair-flood" {
			t.Fatalf("cap2: expected only flood tasks, got %v", instances(got))
		}
	}
}
