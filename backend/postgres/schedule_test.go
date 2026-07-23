package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

func scheduleDSNOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	return dsn
}

func TestSchedule_ClaimAndDedup(t *testing.T) {
	dsn := scheduleDSNOrSkip(t)
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

	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "job1", Cron: "0 * * * *", Workflow: "WF", Queue: "default",
		Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	_, err = b.pool.Exec(ctx, `UPDATE wf_schedules SET next_run_at = now() - interval '1 second' WHERE id = 'job1'`)
	if err != nil {
		t.Fatal(err)
	}

	due, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due) != 1 || !due[0].Created {
		t.Fatalf("first claim: %v %#v", err, due)
	}
	instID := due[0].InstanceID

	_, err = b.pool.Exec(ctx, `UPDATE wf_schedules SET next_run_at = $1 WHERE id = 'job1'`, due[0].ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}
	due2, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due2) != 1 {
		t.Fatalf("second claim: %v %#v", err, due2)
	}
	if due2[0].Created || due2[0].InstanceID != instID {
		t.Fatalf("want dedup Created=false same id, got %+v", due2[0])
	}

	list, err := b.ListInstances(ctx, backend.InstanceFilter{Name: "WF"})
	if err != nil || len(list) != 1 {
		t.Fatalf("instances: %v %#v", err, list)
	}
}
