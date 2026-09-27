package firestore_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestPurgeIDReuseKeepsReplacement covers the purge ID-reuse fence (issue
// #296 follow-up): recreating an instance ID immediately after its purge must
// yield a fully functional replacement — no sweep may delete the new
// incarnation's documents — and purging the replacement later still counts
// exactly one instance.
func TestPurgeIDReuseKeepsReplacement(t *testing.T) {
	emulatorOrSkip(t)
	ctx := context.Background()
	b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const id = "purge-reuse-1"
	completeTerminally(t, b, id)

	n, err := b.PurgeInstances(ctx, 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("after purge: want ErrNotFound, got %v", err)
	}

	// Recreate the same ID: the replacement must be fully functional, i.e.
	// the trailing purge sweep must not have deleted the new incarnation's
	// task, journal, or sequence documents.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate after purge: %v", err)
	}
	inst, err := b.GetInstance(ctx, id)
	if err != nil || inst.Status != "running" {
		t.Fatalf("replacement: %v %#v", err, inst)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "reuse",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("replacement claim: %v %#v", err, tasks)
	}
	events, err := b.GetJournal(ctx, id, 0)
	if err != nil || len(events) != 1 || events[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("replacement journal: %v %#v", err, events)
	}

	// Purging the replacement still counts exactly one instance.
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	n, err = b.PurgeInstances(ctx, 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("repurged %d, want 1", n)
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("after repurge: want ErrNotFound, got %v", err)
	}
}

func completeTerminally(t *testing.T, b *firestore.Backend, id string) {
	t.Helper()
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "reuse-setup",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %s: %v %#v", id, err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  id,
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		Terminal:    &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}); err != nil {
		t.Fatal(err)
	}
}
