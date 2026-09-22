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

// TestCommitWorkflow_ConflictReleasesLease covers the single-commit path:
// a conflicted CommitAdvancement must leave the task immediately
// reclaimable instead of holding the lease until LeaseDuration expiry.
func TestCommitWorkflow_ConflictReleasesLease(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "rl1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	st, err := w.loadWorkflowState(ctx, "rl1")
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 30 * time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	err = w.commitWorkflow(ctx, tasks[0], st.Journal, backend.Advancement{
		InstanceID:  "rl1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq - 1, // stale → ErrConflict
		NewEvents:   []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("got %v", err)
	}
	if _, ok := w.stickyGet("rl1"); ok {
		t.Fatal("sticky should be dropped on conflict")
	}
	// No clock movement: the task must be reclaimable well before the
	// 30s lease expires.
	reclaimed, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 30 * time.Second, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].InstanceID != "rl1" {
		t.Fatalf("want rl1 task immediately reclaimable, got %+v", reclaimed)
	}
}

// failHeadBackend fails workflow-state loads to exercise the
// tickWorkflows handleWorkflow error path.
type failHeadBackend struct {
	backend.Backend
	err error
}

func (f *failHeadBackend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	return nil, f.err
}

// failCommitBackend fails advancement commits to exercise the
// commitWorkflow non-contention error path.
type failCommitBackend struct {
	backend.Backend
	err error
}

func (f *failCommitBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return f.err
}

// TestCommitWorkflow_NonConflictNacksWithDelay covers the commit-failure
// path: a non-contention commit error must nack with a delay instead of
// releasing immediately, so a persistently failing commit does not spin
// the poll loop.
func TestCommitWorkflow_NonConflictNacksWithDelay(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(now)
	fb := &failCommitBackend{Backend: mem, err: errors.New("injected commit failure")}
	w := NewWorker(fb, WorkerOptions{Queues: []string{"default"}})
	if err := fb.CreateInstance(ctx, backend.NewInstance{ID: "nc1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	st, err := w.loadWorkflowState(ctx, "nc1")
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := fb.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 30 * time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	err = w.commitWorkflow(ctx, tasks[0], st.Journal, backend.Advancement{
		InstanceID:  "nc1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents:   []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
	})
	if err == nil || errors.Is(err, backend.ErrConflict) {
		t.Fatalf("want injected commit error, got %v", err)
	}
	claim := func(workerID string) []backend.Task {
		got, err := fb.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{"default"}, Limit: 1,
			Lease: 30 * time.Second, WorkerID: workerID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := claim("w2"); len(got) != 0 {
		t.Fatalf("want nc1 task hidden after non-conflict commit error, got %+v", got)
	}
	mem.SetNow(now.Add(w.opts.IncompatibleRetryDelay + time.Second))
	if got := claim("w3"); len(got) != 1 || got[0].InstanceID != "nc1" {
		t.Fatalf("want nc1 task reclaimable after nack delay, got %+v", got)
	}
}

// TestTickWorkflows_HandleErrorBacksOff covers the worker.go change:
// a non-contention handleWorkflow failure (store error, oversized
// advancement) must be nacked with a delay — not immediately released —
// so a persistently failing task does not reclaim-fail-notify in a tight
// loop. Contention (ErrConflict) keeps the immediate release for fast replay.
func TestTickWorkflows_HandleErrorBacksOff(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newTickWorker := func(failErr error) (*failHeadBackend, *Worker) {
		mem := memory.New()
		mem.SetNow(now)
		fb := &failHeadBackend{Backend: mem, err: failErr}
		w := NewWorker(fb, WorkerOptions{
			Queues:              []string{"default"},
			WorkflowConcurrency: 2,
			ClaimLimit:          10,
			LeaseDuration:       30 * time.Second,
			Metrics:             observability.MustNewMetrics(),
		})
		return fb, w
	}
	claim := func(t *testing.T, fb *failHeadBackend, workerID string) []backend.Task {
		t.Helper()
		tasks, err := fb.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{"default"}, Limit: 1,
			Lease: 30 * time.Second, WorkerID: workerID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tasks
	}

	t.Run("generic error backs off", func(t *testing.T) {
		fb, w := newTickWorker(errors.New("injected load failure"))
		if err := fb.CreateInstance(ctx, backend.NewInstance{ID: "he1", Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
		w.tickWorkflows(ctx)
		// No clock movement: the nacked task must NOT be immediately
		// reclaimable (that immediate re-visibility is what spun the
		// reclaim-fail-notify loop).
		if tasks := claim(t, fb, "w2"); len(tasks) != 0 {
			t.Fatalf("want he1 task hidden after handle error, got %+v", tasks)
		}
		// After the nack delay it becomes reclaimable again.
		fb.Backend.(*memory.Backend).SetNow(now.Add(w.opts.IncompatibleRetryDelay + time.Second))
		if tasks := claim(t, fb, "w3"); len(tasks) != 1 || tasks[0].InstanceID != "he1" {
			t.Fatalf("want he1 task reclaimable after nack delay, got %+v", tasks)
		}
	})

	t.Run("conflict releases immediately", func(t *testing.T) {
		fb, w := newTickWorker(backend.ErrConflict)
		if err := fb.CreateInstance(ctx, backend.NewInstance{ID: "he2", Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
		w.tickWorkflows(ctx)
		// No clock movement: contention can succeed on replay, so the
		// task must be immediately reclaimable.
		if tasks := claim(t, fb, "w2"); len(tasks) != 1 || tasks[0].InstanceID != "he2" {
			t.Fatalf("want he2 task immediately reclaimable after conflict, got %+v", tasks)
		}
	})
}
