package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func openMigrated(t *testing.T) *sqlite.Backend {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "t.db")
	b, err := sqlite.New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSQLite_LeaseHeartbeatReleaseRetry(t *testing.T) {
	ctx := context.Background()
	b := openMigrated(t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	st, err := b.LoadWorkflow(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	seq := st.NextSeq
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: seq,
		NewEvents: []journal.Event{
			{Seq: seq, Type: journal.TypeActivityScheduled, Name: "act", Payload: []byte(`{"name":"act","input":null}`)},
		},
		ActivityTasks: []backend.NewTask{
			{Kind: "activity", InstanceID: "i1", Name: "act", Seq: seq, Queue: "default", Input: []byte("null")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	acts, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(acts) != 1 {
		t.Fatalf("act claim: %v %#v", err, acts)
	}
	aid := acts[0].ID
	if err := b.ExtendLease(ctx, aid, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := b.RecordHeartbeat(ctx, aid, 2*time.Second, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.ReleaseLease(ctx, aid); err != nil {
		t.Fatal(err)
	}
	acts2, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w2",
	})
	if err != nil || len(acts2) != 1 {
		t.Fatalf("reclaim: %v %#v", err, acts2)
	}
	if err := b.RetryActivity(ctx, acts2[0].ID, time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestSQLite_ScheduleGetPauseClaim(t *testing.T) {
	ctx := context.Background()
	b := openMigrated(t)
	if err := b.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly", Cron: "0 * * * *", Workflow: "job", Queue: "default",
		Input: []byte(`{"n":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	s, err := b.GetSchedule(ctx, "hourly")
	if err != nil || s.ID != "hourly" || s.Paused {
		t.Fatalf("%+v %v", s, err)
	}
	if _, err := b.GetSchedule(ctx, "missing"); err == nil {
		t.Fatal("want not found")
	}
	if err := b.PauseSchedule(ctx, "hourly", true); err != nil {
		t.Fatal(err)
	}
	s2, err := b.GetSchedule(ctx, "hourly")
	if err != nil || !s2.Paused {
		t.Fatalf("%+v %v", s2, err)
	}
	due, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("paused should not claim: %v %#v", err, due)
	}
	if err := b.PauseSchedule(ctx, "hourly", false); err != nil {
		t.Fatal(err)
	}
	// Force due by upserting with past next via Pause false then wait — ClaimDueSchedules uses now.
	// Advance by upserting cron that is due immediately is hard; use PauseSchedule false and
	// ClaimDueSchedules after Upsert with next in the past isn't exposed. At least Pause/Get covered.
	_ = due
}
