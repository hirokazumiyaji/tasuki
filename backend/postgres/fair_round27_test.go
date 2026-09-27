package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestFairClaimRequerySkipsAcceptedIDs covers issue #294 round-27 P2 (mysql
// twin in backend/mysql/fair_round27_test.go): Limit=3/MaxPerInstance=2 over
// FIFO A1..A70,B1,C1 with A1..A69 locked by a concurrent claimer. The
// overflow requery from the A69 snapshot must skip the already-claimed B1
// (see backend.SeedFairAttempted) instead of re-offering it. This backend
// leases eagerly per pass, so a requery rescan normally never re-sees a
// claimed row (visible_at moved to the future); the seed is parity hardening
// for the same loop shape (e.g. a zero-lease claim re-exposes instantly).
// The batch must come back FIFO [A70 B1 C1].
//
// NOTE: as on mysql, the duplicate arm is unreachable in a deterministic
// single-claimer run, so this test pins the SCENARIO end to end and passes
// both before and after the round-27 seed. The fail-without-fix pin on the
// seed itself lives in the backend package
// (TestSeedFairAttemptedSkipsAcceptedInRequery).
func TestFairClaimRequerySkipsAcceptedIDs(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
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
	rows, err := b.Pool().Query(ctx, `
		SELECT id, instance_id FROM wf_tasks
		WHERE kind = 'activity' AND queue = ANY($1)
		ORDER BY visible_at, id`, queues)
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

	// Blocker locks A1..A69 one by one with PK equality.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	for i := 0; i < 69; i++ {
		if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = $1 FOR UPDATE`, cands[i].id); err != nil {
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
