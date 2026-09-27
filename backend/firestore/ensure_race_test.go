package firestore

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

func ensureTestBackend(t *testing.T) *Backend {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set")
	}
	b, err := New(context.Background(), os.Getenv("TASUKI_FIRESTORE_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b
}

// taskDocPresent reports whether the singleton workflow task doc exists.
func taskDocPresent(t *testing.T, b *Backend, ctx context.Context, instanceID string) bool {
	t.Helper()
	s, err := b.ref("wf_tasks", wfTaskID(instanceID)).Get(ctx)
	if isNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return s.Exists()
}

// A stale ensure must not recreate the singleton workflow task after the
// terminal sweep deleted it (Codex round 11 on #328, symmetric with the
// DynamoDB round-7 transact-gate): CompleteActivity/FireDueTimers commit the
// inbox and read the instance as running, then a terminal transition commits
// and its sweep deletes every task before the ensure's Create executes. The
// transactional Create (instance still running, re-checked inside the
// transaction) aborts instead of undoing the cleanup. A bare Create
// recreates the row, which lingers with no later pass reaping it.
func TestEnsureWorkflowTaskForced_NoRecreateAfterTerminal(t *testing.T) {
	ctx := context.Background()
	b := ensureTestBackend(t)

	const id = "ensure-race-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if !taskDocPresent(t, b, ctx, id) {
		t.Fatal("setup: expected a workflow task doc before termination")
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	if taskDocPresent(t, b, ctx, id) {
		t.Fatal("setup: terminate must delete the workflow task doc")
	}
	// The stale ensure's Create executes now, after the cleanup committed.
	if err := b.createWorkflowTaskIfRunning(ctx, id); err != nil {
		t.Fatal(err)
	}
	if taskDocPresent(t, b, ctx, id) {
		t.Fatal("stale ensure recreated the workflow task after the terminal sweep deleted it")
	}
}

// The gated ensure still creates the task for a running instance, and a
// second ensure is idempotent success (AlreadyExists inside the transaction
// maps to nil).
func TestEnsureWorkflowTaskForced_CreatesForRunning(t *testing.T) {
	ctx := context.Background()
	b := ensureTestBackend(t)

	const id = "ensure-race-2"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Remove the task doc directly (simulating a sweep) while the instance
	// stays running; the gated Create must recreate it.
	if _, err := b.ref("wf_tasks", wfTaskID(id)).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.ensureWorkflowTaskForced(ctx, id); err != nil {
		t.Fatal(err)
	}
	if !taskDocPresent(t, b, ctx, id) {
		t.Fatal("gated ensure must create the task for a running instance")
	}
	if err := b.ensureWorkflowTaskForced(ctx, id); err != nil {
		t.Fatal(err)
	}
}
