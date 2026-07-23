package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestMemory_ScheduleClaimAndDedup(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)

	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly", Cron: "0 * * * *", Workflow: "job", Queue: "default",
		Input: []byte(`{"n":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	s, err := b.GetSchedule(ctx, "hourly")
	if err != nil {
		t.Fatal(err)
	}
	// Next after 2026-01-01 00:00 is 01:00
	wantFire := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if !s.NextRunAt.Equal(wantFire) {
		t.Fatalf("next=%v want %v", s.NextRunAt, wantFire)
	}

	// Not due yet
	due, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("premature claim: %v %#v", err, due)
	}

	b.SetNow(wantFire)
	due, err = b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("claim: %v %#v", err, due)
	}
	if !due[0].Created || due[0].InstanceID != backend.ScheduleInstanceID("hourly", wantFire) {
		t.Fatalf("due=%+v", due[0])
	}
	inst, err := b.GetInstance(ctx, due[0].InstanceID)
	if err != nil || inst.Name != "job" {
		t.Fatalf("instance: %v %#v", err, inst)
	}

	// Rewind next_run_at by re-upsert with clock still at fire time — simulate double fire:
	// force next_run_at back and claim again → AlreadyExists, Created=false, next advances.
	b.SetNow(wantFire)
	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly", Cron: "0 * * * *", Workflow: "job",
	}); err != nil {
		t.Fatal(err)
	}
	// Upsert recomputes next from now(=wantFire) → 02:00. Set due again via Pause+manual:
	// Claim at 02:00 for a clean second fire, then double-claim same slot by creating conflict.
	b.SetNow(time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC))
	due2, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due2) != 1 || !due2[0].Created {
		t.Fatalf("second hour: %v %#v", err, due2)
	}

	// Double fire same scheduledAt: put next_run_at back to 02:00 while instance exists.
	s2, _ := b.GetSchedule(ctx, "hourly")
	_ = s2
	// Use PauseSchedule path: re-upsert from 01:00 so next is 02:00 again, claim → dedup
	b.SetNow(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly", Cron: "0 * * * *", Workflow: "job",
	}); err != nil {
		t.Fatal(err)
	}
	b.SetNow(time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC))
	due3, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due3) != 1 {
		t.Fatalf("dedup claim: %v %#v", err, due3)
	}
	if due3[0].Created {
		t.Fatal("expected Created=false on duplicate instance id")
	}
	// Still only one instance for that ID
	list, _ := b.ListInstances(ctx, backend.InstanceFilter{Name: "job"})
	ids := map[string]bool{}
	for _, i := range list {
		ids[i.ID] = true
	}
	if len(ids) != 2 { // 01:00 and 02:00 fires
		t.Fatalf("want 2 instances, got %d %#v", len(ids), ids)
	}
}

func TestMemory_PauseSchedule(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.SetNow(now)
	_ = b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "p", Cron: "0 * * * *", Workflow: "job",
	})
	if err := b.PauseSchedule(ctx, "p", true); err != nil {
		t.Fatal(err)
	}
	b.SetNow(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	due, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("paused should not fire: %v %#v", err, due)
	}
}
