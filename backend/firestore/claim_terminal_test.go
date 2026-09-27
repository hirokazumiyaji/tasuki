package firestore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// A Limit:1 poll head-blocked by a terminal-instance task must still reach
// the live task behind it (Codex round 2 on #327). ClaimTasks deletes the
// stale row best-effort and refills the freed slot; without that the same
// stale candidate is returned on every call and live tasks starve.
func TestClaimDeletesTerminalTaskAndRefills(t *testing.T) {
	guardTestEmulator(t)
	ctx := context.Background()
	b, err := New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
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

	const liveID, termID = "claim-term-live", "claim-term-dead"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: liveID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: termID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, termID); err != nil {
		t.Fatal(err)
	}
	// Re-insert a stale task for the terminated instance, older than every
	// other row so it head-blocks the visible_at-ordered claim scan.
	stale := workflowTaskDoc(termID, "default", 1, time.Now().UTC().Add(-time.Hour))
	if _, err := b.ref("wf_tasks", wfTaskID(termID)).Create(ctx, stale); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "claim-term",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].InstanceID != liveID {
		t.Fatalf("Limit:1 claim behind a terminal task returned %v (want the live task)", tasks)
	}
	if snap, err := b.ref("wf_tasks", wfTaskID(termID)).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("stale terminal task was skipped but not deleted")
	}
}
