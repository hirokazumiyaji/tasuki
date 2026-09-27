package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Schema-less (validator-failing) stores must surface the failure through
// StartWithError, and the legacy Start wrapper must leave the worker stopped.
func TestStartWithErrorSchemaFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := &brokenSchema{Backend: memory.New(), fail: true}
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("WF"))

	if err := w.StartWithError(ctx); err == nil {
		t.Fatal("StartWithError: want schema validation error, got nil")
	}
	if w.Running() {
		t.Fatal("Running: want false after failed StartWithError")
	}
	defer w.Shutdown(ctx)

	// The Start wrapper must also leave the worker stopped (logs and returns).
	w.Start(ctx)
	if w.Running() {
		t.Fatal("Running: want false after failed Start")
	}

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("start-err-1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "start-err-1")
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != "running" {
			t.Fatalf("worker processed tasks despite failed validation: %s", info.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Double Start must be detectable: the second StartWithError returns
// ErrWorkerAlreadyRunning and Running stays true until Shutdown.
func TestStartWithErrorDoubleStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := memory.New()
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})

	if w.Running() {
		t.Fatal("Running: want false before start")
	}
	if err := w.StartWithError(ctx); err != nil {
		t.Fatalf("first StartWithError: %v", err)
	}
	if !w.Running() {
		t.Fatal("Running: want true after start")
	}
	if err := w.StartWithError(ctx); !errors.Is(err, worker.ErrWorkerAlreadyRunning) {
		t.Fatalf("second StartWithError: want ErrWorkerAlreadyRunning, got %v", err)
	}
	if !w.Running() {
		t.Fatal("Running: want true after double start")
	}
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if w.Running() {
		t.Fatal("Running: want false after Shutdown")
	}
	// Restart after Shutdown must succeed.
	if err := w.StartWithError(ctx); err != nil {
		t.Fatalf("restart StartWithError: %v", err)
	}
	if !w.Running() {
		t.Fatal("Running: want true after restart")
	}
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
