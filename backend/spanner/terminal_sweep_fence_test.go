package spanner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// seedFenceTimer inserts one timer row for id directly, modeling a timer the
// (old or replacement) incarnation scheduled.
func seedFenceTimer(t *testing.T, b *Backend, ctx context.Context, id string, seq int64) {
	t.Helper()
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_timers", map[string]any{
				"instance_id": id, "seq": seq, "fire_at": nowUTC().Add(time.Hour),
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// instanceVictimTx captures the fence identity the terminal commit observes:
// the pre-commit incarnation (created_at plus the unique token) for the
// post-commit sweep fence.
func instanceVictimTx(t *testing.T, b *Backend, ctx context.Context, id string) purgeVictim {
	t.Helper()
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at", "incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	v := purgeVictim{id: id}
	var incarnation spanner.NullString
	if err := row.Columns(&v.createdAt, &incarnation); err != nil {
		t.Fatal(err)
	}
	if incarnation.Valid {
		v.incarnation = incarnation.StringVal
	}
	return v
}

// fenceChildCount counts rows of one instance in a child table.
func fenceChildCount(t *testing.T, b *Backend, ctx context.Context, table, col, id string) int {
	t.Helper()
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT ` + col + ` FROM ` + table + ` WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	n := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			return n
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
}

// TestTerminalSweepStaleFenceKeepsReplacement covers the Codex round-20 P1
// finding on #296, mirrored from Firestore: PurgeInstances(0, ...) deleting
// a terminal instance while its terminal sweep is paused, then CreateInstance
// reusing the ID, then the sweep resuming. The resumed sweep's page queries
// filter by instance_id only, so without an incarnation fence it deletes the
// replacement's workflow task and timers; the exact-key dedupe sweep likewise
// deletes the replacement's recreated guard (dedupe keys are deterministic),
// stripping it while its inbox event remains so a later retry duplicates the
// signal.
//
// The test drives the real interleaving through the public API — terminal
// commit (sweep paused via flipStatusWithoutSweep), full purge (marker
// cleared), ID reuse with resent DedupeID — then resumes the paused sweep by
// invoking it with the stale pre-commit fence, exactly as the paused sweep
// would. The fenced sweep must abort with a nil return leaving every
// replacement row intact; purge owns the leftovers. Gutting the fence checks
// (keeping the signatures) deletes the replacement's rows and fails the
// test.
func TestTerminalSweepStaleFenceKeepsReplacement(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "terminal-sweep-fence"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Old incarnation rows: the workflow task exists (auto-created); add a
	// timer and a dedupe-guarded signal.
	seedFenceTimer(t, b, ctx, id, 7)
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	// What the terminal commit observes: the pre-commit incarnation plus
	// the exact dedupe key set the post-commit sweep would remove.
	oldVictim := instanceVictimTx(t, b, ctx, id)
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0] != dedupeKey("K") {
		t.Fatalf("dedupe snapshot = %v, want the single guard K", snapshot)
	}

	// Terminal status commits while the sweep is paused; the purge then
	// deletes the instance, reaps every child row, and clears its marker.
	flipStatusWithoutSweep(t, b, ctx, id)
	if n, err := b.PurgeInstances(ctx, 0, nil, 10); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v; want 1, nil", n, err)
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("after purge: want ErrNotFound, got %v", err)
	}

	// Recreate the same ID (allowed: the marker is cleared) and rebuild
	// the replacement's rows. Dedupe keys are deterministic, so the resent
	// guard K recreates the very same row the snapshot names.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate after purge: %v", err)
	}
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	seedFenceTimer(t, b, ctx, id, 8)
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 1 {
		t.Fatalf("setup: replacement tasks = %d, want 1", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 1 {
		t.Fatalf("setup: replacement timers = %d, want 1", n)
	}
	if !b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("setup: replacement guard K missing")
	}

	// The paused sweep resumes with its stale pre-commit fence: it must
	// abort (nil) instead of deleting the replacement's rows.
	if err := b.sweepTerminateDocs(ctx, oldVictim, snapshot); err != nil {
		t.Fatalf("stale terminate sweep: %v (want fenced abort to nil)", err)
	}
	victim := oldVictim
	guard := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, victim) }
	guardTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, victim)
	}
	if err := b.sweepSignalDedupeIDs(ctx, id, guard, guardTx, snapshot); err != nil {
		t.Fatalf("stale dedupe sweep: %v (want fenced abort to nil)", err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 1 {
		t.Fatalf("stale sweep deleted the replacement's workflow task (tasks=%d, want 1)", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 1 {
		t.Fatalf("stale sweep deleted the replacement's timer (timers=%d, want 1)", n)
	}
	if !b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("stale sweep deleted the replacement's guard K")
	}
	if inst, err := b.GetInstance(ctx, id); err != nil || inst.Status != "running" {
		t.Fatalf("replacement instance: %v %#v (want running)", err, inst)
	}

	// Positive control: with the CURRENT incarnation the same sweep still
	// cleans up. Flip the replacement terminal (sweep paused again), sweep
	// with its own fence, and require every row gone.
	flipStatusWithoutSweep(t, b, ctx, id)
	curVictim := instanceVictimTx(t, b, ctx, id)
	curSnapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.sweepTerminateDocs(ctx, curVictim, curSnapshot); err != nil {
		t.Fatal(err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 0 {
		t.Fatalf("current-incarnation sweep left %d task rows behind", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 0 {
		t.Fatalf("current-incarnation sweep left %d timer rows behind", n)
	}
	if b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("current-incarnation sweep left guard K behind")
	}
}
