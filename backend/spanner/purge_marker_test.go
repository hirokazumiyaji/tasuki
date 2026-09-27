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

func TestPurgeMarkerExpired(t *testing.T) {
	now := time.Now().UTC()
	if purgeMarkerExpired(now, time.Time{}) {
		t.Fatal("zero write time must never expire (conservative linger)")
	}
	if purgeMarkerExpired(now, now) {
		t.Fatal("fresh marker must not expire")
	}
	if !purgeMarkerExpired(now, now.Add(-purgeMarkerTTL-time.Hour)) {
		t.Fatal("marker past TTL with a live replacement must expire")
	}
}

// crashPurgeAfterDelete runs the purge first sweep and the victim delete
// (which durably writes the purge marker), then stops — modeling a process
// crash before the second sweep/reap. Stragglers seeded between the sweep
// and the delete model writers committing in that window.
func crashPurgeAfterDelete(t *testing.T, b *Backend, ctx context.Context, id string) purgeVictim {
	t.Helper()
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at"})
	if err != nil {
		t.Fatal(err)
	}
	var createdAt time.Time
	if err := row.Columns(&createdAt); err != nil {
		t.Fatal(err)
	}
	v := purgeVictim{id: id, createdAt: createdAt}
	own := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, v) }
	ownTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, v)
	}
	if err := b.deleteInstanceChildren(ctx, id, own, ownTx); err != nil {
		t.Fatal(err)
	}
	// Post-terminal stragglers: committed after the first sweep, before the
	// delete — exactly the rows the in-memory residual exists to reap.
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "straggler-1"); err != nil {
		t.Fatal(err)
	}
	deleted, _, _, err := b.deletePurgedInstanceRow(ctx, v)
	if err != nil || !deleted {
		t.Fatalf("victim delete: done=%v err=%v", deleted, err)
	}
	// CRASH: no second sweep, no residual reap, no marker clear.
	return v
}

func markerRowMustExist(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	_, err := b.client.Single().ReadRow(ctx, "wf_purge_markers", spanner.Key{id},
		[]string{"instance_id", "created_at", "purged_at"})
	if err != nil {
		t.Fatalf("purge marker for %q must survive the crash (err=%v)", id, err)
	}
}

func childRowsFor(t *testing.T, b *Backend, ctx context.Context, id string) (dedupe, inbox int) {
	t.Helper()
	count := func(sql string) int {
		iter := b.client.Single().Query(ctx, spanner.Statement{
			SQL:    sql,
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
	dedupe = count(`SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`)
	inbox = count(`SELECT id FROM wf_inbox WHERE instance_id = @id`)
	return dedupe, inbox
}

func markerRowGone(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	_, err := b.client.Single().ReadRow(ctx, "wf_purge_markers", spanner.Key{id}, []string{"instance_id"})
	if err == nil {
		t.Fatal("marker must be cleared once the resumed cleanup completes")
	}
	if !isNotFound(err) {
		t.Fatalf("marker read: %v", err)
	}
}

// A crash between the victim-delete commit and the trailing sweep/reap must
// stay recoverable: the durably-written marker lets a later purge resume the
// cleanup where the lost in-memory residual cannot, and an ID-reusing
// replacement must not consume the old rows.
func TestPurgeMarkerCrashResume(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "purge-marker-crash"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	crashPurgeAfterDelete(t, b, ctx, id)
	markerRowMustExist(t, b, ctx, id)

	// The later purge finds no victim (instance row gone) but resumes via
	// the marker: stragglers reaped, marker cleared, nothing counted.
	n, err := b.PurgeInstances(ctx, 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("resume purged %d, want 0 (cleanup completion, not a new victim)", n)
	}
	if dedupe, inbox := childRowsFor(t, b, ctx, id); dedupe+inbox != 0 {
		t.Fatalf("resume left dedupe=%d inbox=%d, want all stragglers reaped", dedupe, inbox)
	}
	markerRowGone(t, b, ctx, id)

	// ID reuse: the replacement must not consume old rows — the straggler
	// DedupeID inserts anew instead of being swallowed by a leftover key.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate after resumed purge: %v", err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "straggler-1"); err != nil {
		t.Fatal(err)
	}
	if _, inbox := childRowsFor(t, b, ctx, id); inbox != 1 {
		t.Fatalf("replacement inbox=%d, want 1 (leftover dedupe key would swallow the send)", inbox)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "reuse",
	})
	if err != nil || len(tasks) != 1 || tasks[0].InstanceID != id {
		t.Fatalf("replacement claim: %v %#v (must be fully functional)", err, tasks)
	}
}

// With a live replacement the straggler rows are ambiguous (old vs the
// replacement's own) and stay for the replacement's own purge; only the
// marker itself ages out via TTL — and even then the rows stay.
// With a live purge marker (crash recovery pending), ID reuse is fenced:
// CreateInstance fails fast instead of letting a replacement consume the
// old incarnation's leftover inbox rows long before its own purge (Codex
// round 11 on #296). The next purge resumes the crashed cleanup via the
// marker (absent instance: rows provably old, swept; marker cleared) and
// unblocks the ID.
func TestPurgeMarkerReplacementDefersToReplacementPurge(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "purge-marker-replacement"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	crashPurgeAfterDelete(t, b, ctx, id)
	markerRowMustExist(t, b, ctx, id)

	// Recreate is fenced while recovery is pending.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); !errors.Is(err, backend.ErrAlreadyExists) {
		t.Fatalf("recreate with live purge marker: err=%v, want ErrAlreadyExists", err)
	}

	// The next purge resumes the crashed cleanup and unblocks the ID:
	// stragglers reaped, marker cleared, nothing counted.
	n, err := b.PurgeInstances(ctx, 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("resume purged %d, want 0 (cleanup completion, not a new victim)", n)
	}
	if dedupe, inbox := childRowsFor(t, b, ctx, id); dedupe+inbox != 0 {
		t.Fatalf("resume left dedupe=%d inbox=%d, want all stragglers reaped", dedupe, inbox)
	}
	markerRowGone(t, b, ctx, id)

	// Recreate now succeeds and the replacement is fully functional.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate after resumed purge: %v", err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "fresh-1"); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "reuse",
	})
	if err != nil || len(tasks) != 1 || tasks[0].InstanceID != id {
		t.Fatalf("replacement claim: %v %#v (must be fully functional)", err, tasks)
	}
}

// clearPurgeMarker removes only the marker for the completed incarnation
// (Codex round 11 on #296): a purge that finishes, loses the ID to a
// replacement, and watches another purge overwrite the marker must not
// delete the newer incarnation's marker. Mismatch leaves the row alone.
func TestClearPurgeMarkerConditional(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "clear-marker-conditional"
	v1 := purgeMarker{id: id, createdAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), purgedAt: nowUTC().Truncate(time.Microsecond)}
	v2 := purgeMarker{id: id, createdAt: v1.createdAt.Add(time.Hour), purgedAt: nowUTC().Truncate(time.Microsecond)}
	writeMarker := func(m purgeMarker) {
		t.Helper()
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite([]*spanner.Mutation{purgeMarkerMutation(
				purgeVictim{id: m.id, createdAt: m.createdAt}, m.purgedAt)})
		}); err != nil {
			t.Fatal(err)
		}
	}
	readMarker := func() (purgeMarker, bool) {
		t.Helper()
		row, err := b.client.Single().ReadRow(ctx, "wf_purge_markers", spanner.Key{id},
			[]string{"instance_id", "created_at", "purged_at"})
		if isNotFound(err) {
			return purgeMarker{}, false
		}
		if err != nil {
			t.Fatal(err)
		}
		var cur purgeMarker
		if err := row.Columns(&cur.id, &cur.createdAt, &cur.purgedAt); err != nil {
			t.Fatal(err)
		}
		return cur, true
	}
	writeMarker(v1)
	// A newer incarnation's purge overwrites the marker (same key).
	writeMarker(v2)
	// The older purge's clear must leave the newer marker alone.
	if err := b.clearPurgeMarker(ctx, v1); err != nil {
		t.Fatal(err)
	}
	cur, ok := readMarker()
	if !ok {
		t.Fatal("conditional clear deleted a newer incarnation's marker")
	}
	if !cur.createdAt.Equal(v2.createdAt) || !cur.purgedAt.Equal(v2.purgedAt) {
		t.Fatalf("marker = %+v, want the newer incarnation's version", cur)
	}
	// Same-incarnation staleness (purged_at differs) also leaves it alone.
	stale := purgeMarker{id: id, createdAt: v2.createdAt, purgedAt: v2.purgedAt.Add(-time.Hour)}
	if err := b.clearPurgeMarker(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMarker(); !ok {
		t.Fatal("purged_at mismatch must leave the marker alone")
	}
	// The owning purge's clear removes exactly its marker.
	if err := b.clearPurgeMarker(ctx, v2); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMarker(); ok {
		t.Fatal("owning purge's clear must remove its marker")
	}
	// Clearing a missing marker is success.
	if err := b.clearPurgeMarker(ctx, v2); err != nil {
		t.Fatal(err)
	}
}

// A stale marker coexisting with a live replacement (pre-fence leftover, or
// a replacement that won the resume-vs-create race) still ages out via TTL —
// and even then only the marker goes, never the ambiguous rows.
func TestPurgeMarkerStaleWithReplacementAgesOut(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "purge-marker-stale-replacement"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	crashPurgeAfterDelete(t, b, ctx, id)
	markerRowMustExist(t, b, ctx, id)

	// Seed a live replacement directly, bypassing the CreateInstance fence:
	// models a pre-fence leftover. Rows stay ambiguous either way.
	now := nowUTC()
	if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_instances", map[string]any{
				"id": id, "name": "WF", "queue": "default", "status": "running",
				"input": jsonVal(nil), "next_seq": int64(2),
				"created_at": now, "updated_at": now,
			}),
		})
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := b.PurgeInstances(ctx, 0, nil, 10); err != nil || n != 0 {
		t.Fatalf("purge with live replacement: n=%d err=%v, want 0 nil", n, err)
	}
	// Rows stay (ambiguous), marker stays (fresh).
	if dedupe, _ := childRowsFor(t, b, ctx, id); dedupe == 0 {
		t.Fatal("ambiguous straggler rows must stay for the replacement's own purge")
	}
	markerRowMustExist(t, b, ctx, id)

	// Backdate the marker past TTL: the next purge drops the metadata but
	// still preserves the rows.
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_purge_markers SET purged_at = @ts WHERE instance_id = @id`,
			Params: map[string]any{"id": id, "ts": time.Now().UTC().Add(-purgeMarkerTTL - time.Hour)},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := b.PurgeInstances(ctx, 0, nil, 10); err != nil || n != 0 {
		t.Fatalf("stale-marker purge: n=%d err=%v, want 0 nil", n, err)
	}
	markerRowGone(t, b, ctx, id)
	if dedupe, _ := childRowsFor(t, b, ctx, id); dedupe == 0 {
		t.Fatal("TTL expiry must drop only the marker, never the ambiguous rows")
	}
	// Replacement still fully functional.
	if _, err := b.GetInstance(ctx, id); err != nil {
		t.Fatalf("replacement must survive: %v", err)
	}
}
