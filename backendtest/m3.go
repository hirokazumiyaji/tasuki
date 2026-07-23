package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// RunM3 adds schedule double-fire dedup coverage.
func RunM3(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("ScheduleDoubleFireDedup", func(t *testing.T) { testScheduleDedup(t, newBackend) })
}

func testScheduleDedup(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "dedup", Cron: "0 * * * *", Workflow: "job", Queue: "default",
	}); err != nil {
		t.Fatal(err)
	}
	fire := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(fire)
	} else {
		// Real-time backends: force due by claiming after upsert next is in future —
		// skip unless clock controllable; memory/sqlite tests cover via factory clock or direct claim.
		t.Skip("schedule dedup conformance requires ClockSetter")
	}

	due1, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due1) != 1 || !due1[0].Created {
		t.Fatalf("first: %v %#v", err, due1)
	}
	// Re-due same slot
	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "dedup", Cron: "0 * * * *", Workflow: "job",
	}); err != nil {
		t.Fatal(err)
	}
	// Upsert from fire time → next is 02:00. Set clock back and force:
	setNow(b, time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC))
	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "dedup", Cron: "0 * * * *", Workflow: "job",
	}); err != nil {
		t.Fatal(err)
	}
	setNow(b, fire)
	due2, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due2) != 1 {
		t.Fatalf("second: %v %#v", err, due2)
	}
	if due2[0].Created {
		t.Fatal("expected dedup Created=false")
	}
	list, err := b.ListInstances(ctx, backend.InstanceFilter{Name: "job"})
	if err != nil || len(list) != 1 {
		t.Fatalf("instances: %v %#v", err, list)
	}
}
