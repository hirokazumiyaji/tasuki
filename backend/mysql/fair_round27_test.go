package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestFairClaimRequerySkipsAcceptedIDs covers issue #294 round-27 P2:
// Limit=3/MaxPerInstance=2 over FIFO A1..A70,B1,C1 with A1..A69 locked by a
// concurrent claimer. Pass 1 secures B1 while the trimmed carry arms an
// overflow requery from the A69 snapshot; the requery must skip the
// already-accepted B1 (see backend.SeedFairAttempted) instead of re-offering
// it — a duplicate fills the picker and the claiming UPDATE rejects it,
// starving C1. The batch must come back FIFO [A70 B1 C1].
//
// NOTE: the duplicate arm is unreachable in a deterministic single-claimer
// run (the forward scan secures every admittable fresh row before any
// requery can arm with room to spare), so this test passes both before and
// after the round-27 seed — it pins the SCENARIO end to end. The
// fail-without-fix pin on the seed itself lives in the backend package
// (TestSeedFairAttemptedSkipsAcceptedInRequery).
func TestFairClaimRequerySkipsAcceptedIDs(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
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

	const qa, qb, qc = "r27-a", "r27-b", "r27-c"
	claimWF := func(id, queue string) backend.Task {
		t.Helper()
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "r27",
		})
		if err != nil || len(wf) != 1 || wf[0].InstanceID != id {
			t.Fatalf("claim wf %s: %v %#v", id, err, wf)
		}
		return wf[0]
	}
	spawn := func(id, queue string, n int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		wf := claimWF(id, queue)
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		adv := backend.Advancement{InstanceID: id, TaskID: wf.ID, ExpectedSeq: st.NextSeq}
		for i := 0; i < n; i++ {
			seq := st.NextSeq + int64(i)
			adv.NewEvents = append(adv.NewEvents, journal.Event{
				Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
			})
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind: "activity", Queue: queue, InstanceID: id, Name: "step",
				Seq: seq, Input: []byte(`{}`),
			})
		}
		if err := b.CommitAdvancement(ctx, adv); err != nil {
			t.Fatal(err)
		}
	}
	// FIFO creation order: A1..A70, B1, C1.
	spawn("r27-A", qa, 70)
	spawn("r27-B", qb, 1)
	spawn("r27-C", qc, 1)

	queues := []string{qa, qb, qc}
	rows, err := b.DB().QueryContext(ctx, `
		SELECT id, instance_id FROM wf_tasks
		WHERE kind = 'activity' AND queue IN (?, ?, ?)
		ORDER BY visible_at, id`, qa, qb, qc)
	if err != nil {
		t.Fatal(err)
	}
	type cand struct {
		id  int64
		ins string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.id, &c.ins); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(cands) != 72 {
		t.Fatalf("activities = %d, want 72", len(cands))
	}
	for i := 0; i < 70; i++ {
		if cands[i].ins != "r27-A" {
			t.Fatalf("cands[%d] = %s, want r27-A", i, cands[i].ins)
		}
	}
	if cands[70].ins != "r27-B" || cands[71].ins != "r27-C" {
		t.Fatalf("tail = %v, want [r27-B r27-C]", cands[70:])
	}

	// Blocker locks A1..A69 one by one with PK equality: a multi-row IN
	// degrades to a range scan under REPEATABLE READ and its next-key locks
	// would spill onto A70, which must stay free.
	btx, err := b.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback()
	for i := 0; i < 69; i++ {
		if _, err := btx.ExecContext(ctx, `SELECT id FROM wf_tasks WHERE id = ? FOR UPDATE`, cands[i].id); err != nil {
			t.Fatal(err)
		}
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: queues, Limit: 3,
		Lease: time.Minute, WorkerID: "r27-w", MaxPerInstance: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("claim = %d tasks, want 3 [A70 B1 C1]", len(tasks))
	}
	want := []int64{cands[69].id, cands[70].id, cands[71].id}
	for i, id := range want {
		if tasks[i].ID != id {
			t.Fatalf("claim[%d] = %d, want %d (FIFO [A70 B1 C1])", i, tasks[i].ID, id)
		}
	}
}
