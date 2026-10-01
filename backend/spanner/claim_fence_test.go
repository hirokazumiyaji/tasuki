package spanner

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// emulatorDSNOrSkip returns the emulator DSN or skips (in-package counterpart
// of the external dsnOrSkip; both packages' test files compile together).
func emulatorDSNOrSkip(t *testing.T) string {
	t.Helper()
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set")
	}
	dsn := os.Getenv("TASUKI_SPANNER_DSN")
	if dsn == "" {
		t.Skip("TASUKI_SPANNER_DSN not set")
	}
	return dsn
}

func fenceTestBackend(t *testing.T) (*Backend, context.Context) {
	t.Helper()
	ctx := context.Background()
	dsn := emulatorDSNOrSkip(t)
	// Recreate BEFORE New: NewClient's session pool opens sessions in the
	// background, and dropping the database afterwards invalidates them
	// ("Session not found" on every later RPC).
	if err := RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return b, ctx
}

// fenceCleanup best-effort removes one instance for re-runs against a
// non-wiped emulator.
func fenceCleanup(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	_ = b.TerminateInstance(ctx, id)
	_, _ = b.PurgeInstances(ctx, 0, nil, 100)
}

func claimOneWorkflow(t *testing.T, b *Backend, ctx context.Context, worker string) backend.Task {
	t.Helper()
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: worker,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("claim = %d tasks, want 1", len(tasks))
	}
	return tasks[0]
}

// flipStatusWithoutSweep commits a terminal status flip without sweeping
// tasks, deterministically modeling the race window between the claim commit
// and a terminal commit landing right after it: the leased task row still
// exists, so only the post-claim fence can drop it.
func flipStatusWithoutSweep(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id":           id,
				"status":       "terminated",
				"updated_at":   nowUTC(),
				"completed_at": nowUTC(),
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func taskRowExists(t *testing.T, b *Backend, ctx context.Context, id int64) bool {
	t.Helper()
	_, err := b.client.Single().ReadRow(ctx, "wf_tasks", spanner.Key{id}, []string{"id"})
	if isNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// TestFenceClaimedTasksDropsPostClaimTerminal covers the round-18 P1 fence:
// a task claimed while running, followed by a terminal commit before the
// worker acts, must be dropped (residue deleted) rather than handed out for
// post-completion invocation. A running instance's task is kept, and a
// missing instance's task is dropped as residue.
func TestFenceClaimedTasksDropsPostClaimTerminal(t *testing.T) {
	b, ctx := fenceTestBackend(t)

	const runningID = "fence-keep"
	fenceCleanup(t, b, ctx, runningID)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: runningID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	keptTask := claimOneWorkflow(t, b, ctx, "fence-w1")
	if keptTask.InstanceID != runningID {
		t.Fatalf("claimed instance = %q, want %q", keptTask.InstanceID, runningID)
	}
	kept, err := b.fenceClaimedTasks(ctx, []backend.Task{keptTask})
	if err != nil || len(kept) != 1 {
		t.Fatalf("fence over running instance = %d tasks err=%v, want 1 nil", len(kept), err)
	}

	const terminalID = "fence-drop"
	fenceCleanup(t, b, ctx, terminalID)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: terminalID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	dropTask := claimOneWorkflow(t, b, ctx, "fence-w2")
	if dropTask.InstanceID != terminalID {
		// The running instance's leased task is invisible; the claim must
		// have picked the new instance's task.
		t.Fatalf("claimed instance = %q, want %q", dropTask.InstanceID, terminalID)
	}
	flipStatusWithoutSweep(t, b, ctx, terminalID)
	kept, err = b.fenceClaimedTasks(ctx, []backend.Task{dropTask})
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 0 {
		t.Fatalf("fence over post-claim terminal = %d tasks, want 0 (must not hand out)", len(kept))
	}
	if taskRowExists(t, b, ctx, dropTask.ID) {
		t.Fatal("terminal residue row must be deleted by the fence")
	}

	// A missing instance is terminal residue too.
	ghost := backend.Task{ID: 918273645, InstanceID: "fence-ghost"}
	kept, err = b.fenceClaimedTasks(ctx, []backend.Task{ghost})
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 0 {
		t.Fatalf("fence over missing instance = %d tasks, want 0", len(kept))
	}
}

// TestReleaseClaimedLeasesFreesForReclaim covers the failure branch of the
// fence: a status-read error must not abandon committed leases for the full
// lease duration. Releasing a live lease makes it immediately claimable.
func TestReleaseClaimedLeasesFreesForReclaim(t *testing.T) {
	b, ctx := fenceTestBackend(t)

	const id = "fence-release"
	fenceCleanup(t, b, ctx, id)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	got := claimOneWorkflow(t, b, ctx, "fence-w3")
	// Still leased: nobody else can claim it.
	again, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "fence-w4",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range again {
		if task.ID == got.ID {
			t.Fatal("leased task must stay hidden before release")
		}
	}
	b.releaseClaimedLeases(ctx, []backend.Task{got})
	reclaimed, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "fence-w5",
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range reclaimed {
		if task.ID == got.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("released lease must be immediately reclaimable")
	}
}
