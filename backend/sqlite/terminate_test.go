package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// TestTerminateInstanceDeleteErrorRollsBack injects a DELETE failure on
// wf_tasks (simulating deadlock / connection loss / ctx timeout surfacing as
// a DELETE error) and requires TerminateInstance to report the error and roll
// back the status update instead of committing a half-terminated instance.
func TestTerminateInstanceDeleteErrorRollsBack(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "terminate.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "term-del-fail", Name: "wf", Queue: "default",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := b.DB().ExecContext(ctx, `
		CREATE TRIGGER terminate_fail_tasks BEFORE DELETE ON wf_tasks
		BEGIN
			SELECT RAISE(ABORT, 'injected terminate delete failure');
		END`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = b.DB().ExecContext(context.Background(), `DROP TRIGGER IF EXISTS terminate_fail_tasks`)
	})

	if err := b.TerminateInstance(ctx, "term-del-fail"); err == nil {
		t.Fatal("TerminateInstance must return the DELETE error")
	}

	inst, err := b.GetInstance(ctx, "term-del-fail")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "running" {
		t.Fatalf("status must stay running after rollback, got %q", inst.Status)
	}
	counts, err := b.CountClaimableTasks(ctx, "workflow", []string{"default"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["default"] != 1 {
		t.Fatalf("workflow task must survive rollback, counts=%v", counts)
	}
}

// TestTerminateInstanceCancelledContext requires a cancelled context to fail
// TerminateInstance without changing the instance status.
func TestTerminateInstanceCancelledContext(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "terminate-ctx.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "term-cancel", Name: "wf", Queue: "default",
	}); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := b.TerminateInstance(cctx, "term-cancel"); err == nil {
		t.Fatal("TerminateInstance with cancelled ctx must return an error")
	}

	inst, err := b.GetInstance(ctx, "term-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "running" {
		t.Fatalf("status must stay running after cancelled terminate, got %q", inst.Status)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("workflow task must survive cancelled terminate: %v n=%d", err, len(tasks))
	}
}
