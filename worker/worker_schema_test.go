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

// brokenSchema fronts a memory backend with a controllable SchemaValidator.
type brokenSchema struct {
	*memory.Backend
	fail bool
}

func (b *brokenSchema) ValidateSchema(ctx context.Context) error {
	if b.fail {
		return errors.New("schema is missing tables wf_tasks, wf_journal")
	}
	return nil
}

// A worker whose backend fails schema validation must not process any task.
func TestWorkerRefusesStartOnSchemaError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := &brokenSchema{Backend: memory.New(), fail: true}
	w := worker.NewWorker(b, worker.WorkerOptions{PollInterval: time.Millisecond})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("WF"))

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("schema-1")); err != nil {
		t.Fatal(err)
	}

	w.Start(ctx)
	defer w.Shutdown(ctx)

	// Give a (buggy) worker ample chances to claim; the instance must stay running.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "schema-1")
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != "running" {
			t.Fatalf("worker processed tasks despite failed validation: %s", info.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Disabling validation restores the old behavior: the loop starts even when
// the validator would fail.
func TestWorkerSchemaValidationDisable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := &brokenSchema{Backend: memory.New(), fail: true}
	w := worker.NewWorker(b, worker.WorkerOptions{
		PollInterval:            time.Millisecond,
		DisableSchemaValidation: true,
	})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, worker.WithName("WF"))

	c := client.NewClient(b)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("schema-2")); err != nil {
		t.Fatal(err)
	}

	w.Start(ctx)
	defer w.Shutdown(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "schema-2")
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == "completed" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("worker with disabled validation did not complete workflow")
}

// worker.ValidateSchema is a no-op for backends without a validator and
// forwards to ValidateSchema when implemented.
func TestValidateSchemaHelper(t *testing.T) {
	ctx := context.Background()
	if err := worker.ValidateSchema(ctx, memory.New()); err != nil {
		t.Fatalf("memory backend: %v", err)
	}
	if err := worker.ValidateSchema(ctx, &brokenSchema{Backend: memory.New(), fail: true}); err == nil {
		t.Fatal("want validator error")
	}
}
