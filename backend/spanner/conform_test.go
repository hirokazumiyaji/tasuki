package spanner_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func TestSmokeCreateClaimCommit(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	if err := spanner.RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := spanner.New(ctx, dsn)
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
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "smoke-1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, "smoke-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "smoke-1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		Terminal:    &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, "smoke-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Status != "completed" {
		t.Fatalf("status=%s", inst.Status)
	}
}

func TestConformance(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	if err := spanner.RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	root, err := spanner.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	if err := root.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()
		if err := root.Reset(ctx); err != nil {
			t.Fatal(err)
		}
		return root
	})
}
