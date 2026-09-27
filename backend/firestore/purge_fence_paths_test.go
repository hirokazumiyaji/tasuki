package firestore

import (
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// A child (or Continue-As-New successor, which is also an adv.Children
// entry) reusing a purged ID while its purge marker is live must fail the
// advancement instead of recreating the instance over pending old rows
// (Codex round 12 on #296): only direct CreateInstance fenced ID reuse, so
// writeAdvancementTx recreated marked IDs and the replacement consumed
// purged signals. On the unfenced code the commit succeeds and this fails.
func TestChildCreationFencedByPurgeMarker(t *testing.T) {
	b, ctx := commitTerminateTestBackend(t)

	const parent = "fence-child-parent"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: parent, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	st, err := b.LoadWorkflowHead(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	// A live purge marker for the child ID: models a crashed purge whose
	// victim delete committed but whose trailing sweep never ran.
	const child = "fence-child-marked"
	if _, err := b.ref(purgeMarkersCollection, child).Create(ctx, purgeMarkerDoc(
		purgeVictim{id: child, createdAt: nowUTC()}, nowUTC())); err != nil {
		t.Fatal(err)
	}
	adv := backend.Advancement{
		InstanceID: parent, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		Children: []backend.NewInstance{{
			ID: child, Name: "WF", Queue: "default", Input: []byte(`{}`), ParentID: parent,
		}},
	}
	if err := b.CommitAdvancement(ctx, adv); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("child commit over live purge marker: err=%v, want ErrConflict", err)
	}
	if _, err := b.GetInstance(ctx, child); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("fenced child must not exist, got err=%v", err)
	}
	// After purge recovery clears the marker, the same advancement commits.
	if _, err := b.ref(purgeMarkersCollection, child).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatalf("child commit after marker clear: %v", err)
	}
	if _, err := b.GetInstance(ctx, child); err != nil {
		t.Fatalf("child must exist after marker clear: %v", err)
	}
}

// A schedule fire reusing a purged ID while its purge marker is live must be
// skipped instead of recreating the instance over pending old rows (Codex
// round 12 on #296): the schedule-claim creation path bypassed the fence.
// On the unfenced code the fire creates the instance and this fails.
func TestScheduleClaimFencedByPurgeMarker(t *testing.T) {
	b, ctx := commitTerminateTestBackend(t)

	// Seed a due schedule directly: this backend has no clock control, so
	// next_run_at goes in the past and the instance ID is derived from the
	// stored value.
	const sched = "fence-sched"
	due := nowUTC().Add(-time.Hour)
	if _, err := b.ref("wf_schedules", sched).Create(ctx, map[string]any{
		"id": sched, "cron": "0 * * * *", "workflow": "job", "queue": "default",
		"input": "", "next_run_at": due, "paused": false,
		"created_at": nowUTC(), "updated_at": nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := b.ref("wf_schedules", sched).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scheduled := timestamp(snap.Data(), "next_run_at")
	instanceID := backend.ScheduleInstanceID(sched, scheduled)
	// A live purge marker for the would-be instance: the previous
	// incarnation's purge crashed before its trailing sweep.
	if _, err := b.ref(purgeMarkersCollection, instanceID).Create(ctx, purgeMarkerDoc(
		purgeVictim{id: instanceID, createdAt: nowUTC()}, nowUTC())); err != nil {
		t.Fatal(err)
	}
	// The fenced fire is skipped: no error, no instance, cursor untouched
	// (so the fire is retried after recovery instead of lost).
	due1, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due1) != 0 {
		t.Fatalf("fenced claim returned %d fires, want 0 (fire deferred past recovery)", len(due1))
	}
	if _, err := b.GetInstance(ctx, instanceID); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("fenced schedule must not create the instance, got err=%v", err)
	}
	cur, err := b.GetSchedule(ctx, sched)
	if err != nil {
		t.Fatal(err)
	}
	if !cur.NextRunAt.Equal(scheduled) {
		t.Fatalf("fenced claim advanced next_run_at to %v (fire would be lost)", cur.NextRunAt)
	}
	// After purge recovery clears the marker, the fire claims normally.
	if _, err := b.ref(purgeMarkersCollection, instanceID).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	due2, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due2) != 1 || !due2[0].Created || due2[0].InstanceID != instanceID {
		t.Fatalf("claim after marker clear: %v %#v, want one Created fire", err, due2)
	}
}
