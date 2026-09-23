package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// RunFairDispatch exercises ClaimRequest.MaxPerInstance on backends that
// implement fair dispatch (postgres, mysql, sqlite, memory, dynamodb,
// firestore).
//
// Backends that do not advertise Capabilities.FairDispatch (e.g. spanner)
// skip explicitly instead of failing on FIFO behavior.
//
// The scenario is head-of-line blocking: instance "fair-flood" enqueues five
// activity tasks before two single-task instances. A capped batch must
// interleave the victims; the uncapped batch stays strict FIFO.
func RunFairDispatch(t *testing.T, newBackend Factory) {
	t.Helper()
	runFairDispatch(t, newBackend)
}

// testFairDispatchGated is the Run-integrated form (#299 item 4): backends
// that ignore MaxPerInstance (DynamoDB, Firestore, Spanner) skip loudly
// instead of silently passing with strict-FIFO behavior (see #297).
func testFairDispatchGated(t *testing.T, newBackend Factory) {
	t.Helper()
	b := newBackend(t)
	if !b.Capabilities().FairDispatch {
		t.Skip("fair dispatch (MaxPerInstance) not implemented by this backend (see #297)")
	}
	runFairDispatch(t, func(t *testing.T) backend.Backend { return b })
}

func runFairDispatch(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	if !b.Capabilities().FairDispatch {
		t.Skip("backend does not support fair dispatch (Capabilities.FairDispatch=false)")
	}
	setNow(b, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))

	const queue = "fq"
	spawn := func(id string, activities int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		// Backends with tight advancement budgets (e.g. DynamoDB's
		// transaction item limit) cannot enqueue a large flood in one
		// advancement (each activity costs a journal write plus a task
		// write). Chunk the flood, chaining ExpectedSeq and forcing a
		// follow-up workflow task between chunks, so every backend
		// enqueues the same FIFO scenario.
		const chunk = 25
		for done := 0; done < activities; {
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
			n := activities - done
			if n > chunk {
				n = chunk
			}
			adv := backend.Advancement{
				InstanceID:  id,
				TaskID:      tasks[0].ID,
				ExpectedSeq: st.NextSeq,
				// Keep the singleton workflow task alive between chunks.
				EnsureWorkflowTask: done+n < activities,
			}
			for i := 0; i < n; i++ {
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
			done += n
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

	// A flood longer than one candidate page (FairOverfetch(3) is 64) must
	// not hide the victims: fair claiming pages until the batch fills.
	spawn("fair-flood2", 70)
	spawn("fair-vc", 1)
	spawn("fair-vd", 1)
	got = claim(3, 1)
	if len(got) != 3 {
		t.Fatalf("paged: want 3 tasks, got %v", instances(got))
	}
	seen = map[string]bool{}
	for _, id := range instances(got) {
		seen[id] = true
	}
	for _, want := range []string{"fair-flood2", "fair-vc", "fair-vd"} {
		if !seen[want] {
			t.Fatalf("paged: %v missing %s", got, want)
		}
	}
}
