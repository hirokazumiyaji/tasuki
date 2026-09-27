package firestore

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func parentEnsureTestBackend(t *testing.T) *Backend {
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
	// No Reset: the shared emulator may host concurrent suites; tests use
	// unique instance IDs plus scoped cleanup instead.
	return b
}

// TestCommitTerminalChildEnsuresParentDespiteCleanupError covers the
// round-18 P2 ordering: a terminal child's ParentNotify inbox row rides the
// transaction, so when the post-commit terminal sweep fails the parent
// workflow-task ensure must still run before the cleanup error surfaces.
// Returning before it leaves the client retry conflicting (task/seq
// consumed) with the parent dormant until the orphan scan.
func TestCommitTerminalChildEnsuresParentDespiteCleanupError(t *testing.T) {
	ctx := context.Background()
	b := parentEnsureTestBackend(t)

	const parentID = "r18-parent"
	childID := parentID + ":child"
	for _, id := range []string{parentID, childID} {
		_, _ = b.ref("wf_instances", id).Delete(ctx)
		_, _ = b.ref("wf_tasks", wfTaskID(id)).Delete(ctx)
		_, _ = b.ref("wf_inbox_seq", id).Delete(ctx)
		it := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
		for {
			d, err := it.Next()
			if err != nil {
				break
			}
			_, _ = d.Ref.Delete(ctx)
		}
		it.Stop()
		jit := b.col("wf_journal").Where("instance_id", "==", id).Documents(ctx)
		for {
			d, err := jit.Next()
			if err != nil {
				break
			}
			_, _ = d.Ref.Delete(ctx)
		}
		jit.Stop()
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: parentID, Name: "Parent", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ptasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
	})
	if err != nil || len(ptasks) != 1 || ptasks[0].InstanceID != parentID {
		t.Fatalf("claim parent: %v %#v", err, ptasks)
	}
	pst, err := b.LoadWorkflow(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	childSeq := pst.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  parentID,
		TaskID:      ptasks[0].ID,
		ExpectedSeq: childSeq,
		NewEvents: []journal.Event{{
			Seq: childSeq, Type: journal.TypeChildScheduled, Name: "Child",
		}},
		Children: []backend.NewInstance{{
			ID: childID, Name: "Child", Queue: "default",
			ParentID: parentID, ParentSeq: childSeq,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	ctasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	var childTask backend.Task
	for _, task := range ctasks {
		if task.InstanceID == childID {
			childTask = task
		}
	}
	if childTask.ID == 0 {
		t.Fatal("setup: child workflow task not claimable")
	}
	cst, err := b.LoadWorkflow(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}

	// Fault-inject a terminal-sweep failure.
	boom := errors.New("boom: terminal sweep failed")
	oldCleanup := terminalCleanupFunc
	terminalCleanupFunc = func(*Backend, context.Context, string) error { return boom }
	defer func() { terminalCleanupFunc = oldCleanup }()

	result := []byte(`"ok"`)
	commitErr := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  childID,
		TaskID:      childTask.ID,
		ExpectedSeq: cst.NextSeq,
		NewEvents: []journal.Event{{
			Seq: cst.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: result,
		}},
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: result},
		ParentNotify: &journal.Event{
			Type: journal.TypeChildCompleted, RefSeq: childSeq, Payload: result,
		},
	})
	if commitErr == nil || !strings.Contains(commitErr.Error(), "boom") {
		t.Fatalf("commit = %v, want the surfaced cleanup error", commitErr)
	}
	// The parent ensure ran despite the cleanup failure: the parent owns a
	// workflow task doc again (its own was consumed scheduling the child).
	if !taskDocPresent(t, b, ctx, parentID) {
		t.Fatal("parent workflow task missing: the ensure must run despite the cleanup error")
	}
}
