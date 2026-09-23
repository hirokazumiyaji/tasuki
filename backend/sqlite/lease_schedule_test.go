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
	if err := b.ExtendLease(ctx, acts[0], 2*time.Second); err != nil {
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
	if err := b.RetryActivity(ctx, acts2[0].ID, time.Second); err != nil {
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

	past := time.Now().UTC().Add(-time.Hour).Format("2006-01-02T15:04:05.000000000Z")
	if _, err := b.DB().ExecContext(ctx, `UPDATE wf_schedules SET next_run_at = ? WHERE id = ?`, past, "hourly"); err != nil {
		t.Fatal(err)
	}
	// limit<=0 defaults to 1
	due, err = b.ClaimDueSchedules(ctx, 0)
	if err != nil || len(due) != 1 || !due[0].Created {
		t.Fatalf("first claim: %v %#v", err, due)
	}
	instID := due[0].InstanceID
	schedAt := due[0].ScheduledAt.UTC().Format("2006-01-02T15:04:05.000000000Z")
	if _, err := b.DB().ExecContext(ctx, `UPDATE wf_schedules SET next_run_at = ? WHERE id = ?`, schedAt, "hourly"); err != nil {
		t.Fatal(err)
	}
	due2, err := b.ClaimDueSchedules(ctx, 10)
	if err != nil || len(due2) != 1 {
		t.Fatalf("dedup claim: %v %#v", err, due2)
	}
	if due2[0].Created || due2[0].InstanceID != instID {
		t.Fatalf("want Created=false same id, got %+v", due2[0])
	}
	list, err := b.ListInstances(ctx, backend.InstanceFilter{Name: "job"})
	if err != nil || len(list) != 1 {
		t.Fatalf("instances: %v %#v", err, list)
	}
}

func TestSQLite_CompleteActivity(t *testing.T) {
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
	if err := b.CompleteActivity(ctx, acts[0].ID, journal.Event{
		Type: journal.TypeActivityCompleted, Name: "act", Payload: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.CompleteActivity(ctx, acts[0].ID, journal.Event{Type: journal.TypeActivityCompleted}); err != backend.ErrSuperseded {
		t.Fatalf("want superseded, got %v", err)
	}
	if err := b.CompleteActivity(ctx, 99999, journal.Event{Type: journal.TypeActivityCompleted}); err != backend.ErrSuperseded {
		t.Fatalf("missing want superseded, got %v", err)
	}
	if err := b.RetryActivity(ctx, 99999, 0); err != backend.ErrNotFound {
		t.Fatalf("retry missing: %v", err)
	}
}