package backendtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func testActivityClaimFencing(t *testing.T, newBackend Factory) {
	t.Run("stale completion", func(t *testing.T) {
		ctx := context.Background()
		b := newBackend(t)
		stale := createClaimedActivity(t, b, "fence-complete", "worker-1")
		if err := b.ReleaseLease(ctx, stale); err != nil {
			t.Fatal(err)
		}
		successor := claimActivity(t, b, "worker-2")
		if successor.WorkerID == stale.WorkerID || successor.Attempt == stale.Attempt {
			t.Fatalf("claim did not advance: stale=%+v successor=%+v", stale, successor)
		}

		if err := b.CompleteActivity(ctx, stale, journal.Event{Type: journal.TypeActivityCompleted}); !errors.Is(err, backend.ErrSuperseded) {
			t.Fatalf("stale completion: got %v, want ErrSuperseded", err)
		}
		if _, err := b.GetInstance(ctx, "fence-complete"); err != nil {
			t.Fatal(err)
		}
		third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "activity", Queues: []string{"default"}, Limit: 1,
			Lease: time.Hour, WorkerID: "worker-3",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(third) != 0 {
			t.Fatalf("stale completion removed successor task: %+v", third)
		}
	})

	t.Run("stale retry", func(t *testing.T) {
		ctx := context.Background()
		b := newBackend(t)
		stale := createClaimedActivity(t, b, "fence-retry", "worker-1")
		if err := b.ReleaseLease(ctx, stale); err != nil {
			t.Fatal(err)
		}
		successor := claimActivity(t, b, "worker-2")
		if successor.WorkerID == stale.WorkerID || successor.Attempt == stale.Attempt {
			t.Fatalf("claim did not advance: stale=%+v successor=%+v", stale, successor)
		}

		if err := b.RetryActivity(ctx, stale, time.Hour); !errors.Is(err, backend.ErrSuperseded) {
			t.Fatalf("stale retry: got %v, want ErrSuperseded", err)
		}
		third, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "activity", Queues: []string{"default"}, Limit: 1,
			Lease: time.Hour, WorkerID: "worker-3",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(third) != 0 {
			t.Fatalf("stale retry cleared successor lease: %+v", third)
		}
	})
}

func createClaimedActivity(t *testing.T, b backend.Backend, instanceID, workerID string) backend.Task {
	t.Helper()
	ctx := context.Background()
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: instanceID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	workflowTasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Hour, WorkerID: workerID,
	})
	if err != nil || len(workflowTasks) != 1 {
		t.Fatalf("claim workflow: %v %+v", err, workflowTasks)
	}
	state, err := b.LoadWorkflow(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  instanceID,
		TaskID:      workflowTasks[0].ID,
		ExpectedSeq: state.NextSeq,
		WorkerID:    workflowTasks[0].WorkerID,
		Attempt:     workflowTasks[0].Attempt,
		NewEvents: []journal.Event{{
			Seq: state.NextSeq, Type: journal.TypeActivityScheduled, Name: "act",
		}},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: instanceID,
			Name: "act", Seq: state.NextSeq,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	return claimActivity(t, b, workerID)
}

func claimActivity(t *testing.T, b backend.Backend, workerID string) backend.Task {
	t.Helper()
	tasks, err := b.ClaimTasks(context.Background(), backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1,
		Lease: time.Hour, WorkerID: workerID,
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim activity for %s: %v %+v", workerID, err, tasks)
	}
	return tasks[0]
}
