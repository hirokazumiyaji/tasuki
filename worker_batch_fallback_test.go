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

// TestTickWorkflows_HandleErrorReleasesLease covers the worker.go change:
// a handleWorkflow failure must release the task lease so the next tick
// (not the lease expiry) retries the task.
func TestTickWorkflows_HandleErrorReleasesLease(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fb := &failHeadBackend{Backend: mem, err: errors.New("injected load failure")}
	w := NewWorker(fb, WorkerOptions{
		Queues:              []string{"default"},
		WorkflowConcurrency: 2,
		ClaimLimit:          10,
		LeaseDuration:       30 * time.Second,
		Metrics:             observability.MustNewMetrics(),
	})
	if err := fb.CreateInstance(ctx, backend.NewInstance{ID: "he1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	w.tickWorkflows(ctx)
	// No clock movement: the failed task must be immediately reclaimable.
	tasks, err := fb.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 30 * time.Second, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].InstanceID != "he1" {
		t.Fatalf("want he1 task immediately reclaimable after handle error, got %+v", tasks)
	}
}
