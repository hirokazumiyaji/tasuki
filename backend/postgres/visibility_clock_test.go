package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestNackTask_VisibleAtRelativeToDBNow(t *testing.T) {
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	b, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "vis-nack", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(tasks))
	}

	delay := 30 * time.Second
	if err := b.NackTask(ctx, tasks[0], delay); err != nil {
		t.Fatal(err)
	}

	again, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("claimed %d want 0 while nacked with delay", len(again))
	}

	var ahead time.Duration
	if err := b.pool.QueryRow(ctx, `
		SELECT visible_at - now() FROM wf_tasks WHERE id = $1`, tasks[0].ID).Scan(&ahead); err != nil {
		t.Fatal(err)
	}
	if ahead < delay-2*time.Second || ahead > delay+2*time.Second {
		t.Fatalf("visible_at - now() = %v, want ~%v (DB clock)", ahead, delay)
	}
}

func TestRetryActivity_VisibleAtRelativeToDBNow(t *testing.T) {
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	b, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "vis-retry", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	wtasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(wtasks) != 1 {
		t.Fatalf("claim wf: %v len=%d", err, len(wtasks))
	}
	st, err := b.LoadWorkflow(ctx, "vis-retry")
	if err != nil {
		t.Fatal(err)
	}
	seq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "vis-retry", TaskID: wtasks[0].ID, ExpectedSeq: seq,
		NewEvents: []journal.Event{{Seq: seq, Type: journal.TypeActivityScheduled, Name: "act"}},
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: "vis-retry", Name: "act", Seq: seq,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	acts, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(acts) != 1 {
		t.Fatalf("claim act: %v len=%d", err, len(acts))
	}

	delay := 30 * time.Second
	if err := b.RetryActivity(ctx, acts[0], delay); err != nil {
		t.Fatal(err)
	}

	again, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("claimed %d want 0 while retrying with delay", len(again))
	}

	var ahead time.Duration
	if err := b.pool.QueryRow(ctx, `
		SELECT visible_at - now() FROM wf_tasks WHERE id = $1`, acts[0].ID).Scan(&ahead); err != nil {
		t.Fatal(err)
	}
	if ahead < delay-2*time.Second || ahead > delay+2*time.Second {
		t.Fatalf("visible_at - now() = %v, want ~%v (DB clock)", ahead, delay)
	}
}
