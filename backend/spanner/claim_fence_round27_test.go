package spanner

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestFenceClaimedTasksBatchesDistinctInstances covers the round-27 P1
// fence shape on #291: the post-claim fence must validate a whole batch
// against one status snapshot — tasks of a still-running instance are kept
// while a concurrently-terminated sibling's task is dropped and its residue
// deleted, even though both rode in on the same claim. Fail-without-fix at
// the mapping level: if the batch lookup keys statuses by anything but the
// task's own instance (or drops the per-task mapping), the wrong task is
// kept or the residue survives.
func TestFenceClaimedTasksBatchesDistinctInstances(t *testing.T) {
	b, ctx := fenceTestBackend(t)

	const keepID = "fence-batch-keep"
	const dropID = "fence-batch-drop"
	fenceCleanup(t, b, ctx, keepID)
	fenceCleanup(t, b, ctx, dropID)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: keepID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: dropID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 2,
		Lease: time.Minute, WorkerID: "fence-batch-w",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("claim = %d tasks, want 2 (one per running instance)", len(tasks))
	}
	byInst := map[string]backend.Task{}
	for _, tk := range tasks {
		byInst[tk.InstanceID] = tk
	}
	if _, ok := byInst[keepID]; !ok {
		t.Fatalf("claim instances = %v, want both %q and %q", byInst, keepID, dropID)
	}
	if _, ok := byInst[dropID]; !ok {
		t.Fatalf("claim instances = %v, want both %q and %q", byInst, keepID, dropID)
	}

	// Terminal commit lands after the claim commit: the sweep cannot recall
	// the leased row, only the fence can drop it.
	flipStatusWithoutSweep(t, b, ctx, dropID)

	kept, err := b.fenceClaimedTasks(ctx, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0].InstanceID != keepID {
		t.Fatalf("fence kept %v, want exactly the %q task", kept, keepID)
	}
	if taskRowExists(t, b, ctx, byInst[dropID].ID) {
		t.Fatal("terminal residue row must be deleted by the fence")
	}
	if !taskRowExists(t, b, ctx, byInst[keepID].ID) {
		t.Fatal("running instance's leased row must survive the fence")
	}

	// Empty batch is a no-op (no status RPC at all).
	kept, err = b.fenceClaimedTasks(context.Background(), nil)
	if err != nil || len(kept) != 0 {
		t.Fatalf("fence(nil) = %v %v, want empty nil", kept, err)
	}
}
