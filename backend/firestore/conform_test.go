package firestore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func newBackend(t *testing.T) *firestore.Backend {
	t.Helper()
	emulatorOrSkip(t)
	b, err := firestore.New(context.Background(), os.Getenv("TASUKI_FIRESTORE_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSmokeCreateClaimCommit(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "smoke-1", Name: "WF"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1"})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, "smoke-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{InstanceID: "smoke-1", TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq, Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)}}); err != nil {
		t.Fatal(err)
	}
}

func TestConformance(t *testing.T) {
	root := newBackend(t)
	ctx := context.Background()
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()
		if err := root.Reset(ctx); err != nil {
			t.Fatal(err)
		}
		return root
	})
}
