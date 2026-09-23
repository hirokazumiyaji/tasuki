package firestore

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// purgeMarkerDoc carries the victim identity, incarnation, and delete time.
// Pure shape pin: the resume path keys everything off these fields.
func TestPurgeMarkerDoc(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	now := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	m := purgeMarkerDoc(purgeVictim{id: "v1", createdAt: created}, now)
	if m["instance_id"] != "v1" || !m["created_at"].(time.Time).Equal(created) || !m["purged_at"].(time.Time).Equal(now) {
		t.Fatalf("marker doc = %#v, want victim identity + incarnation + delete time", m)
	}
	marker, ok := decodePurgeMarker("v1", m)
	if !ok || marker.id != "v1" || !marker.createdAt.Equal(created) || !marker.purgedAt.Equal(now) {
		t.Fatalf("decode = %+v %v, want the encoded marker", marker, ok)
	}
	if _, ok := decodePurgeMarker("", map[string]any{}); ok {
		t.Fatal("empty doc must not decode to a marker")
	}
}

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
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v := purgeVictim{id: id, createdAt: timestamp(snap.Data(), "created_at")}
	if err := b.purgeInstanceDocs(ctx, purgeFence{victim: v}); err != nil {
		t.Fatal(err)
	}
	// Post-terminal stragglers: committed after the first sweep, before the
	// delete — exactly the rows the in-memory residual exists to reap.
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "straggler-1"); err != nil {
		t.Fatal(err)
	}
	deleted, _, err := b.deletePurgedInstanceDoc(ctx, v)
	if err != nil || !deleted {
		t.Fatalf("victim delete: done=%v err=%v", deleted, err)
	}
	// CRASH: no second sweep, no residual reap, no marker clear.
	return v
}

func markerDocMustExist(t *testing.T, b *Backend, ctx context.Context, id string) {
	t.Helper()
	msnap, err := b.ref(purgeMarkersCollection, id).Get(ctx)
	if err != nil || !msnap.Exists() {
		t.Fatalf("purge marker for %q must survive the crash (err=%v)", id, err)
	}
}

func childRowsFor(t *testing.T, b *Backend, ctx context.Context, id string) (dedupe, inbox int) {
	t.Helper()
	ids, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	dedupe = len(ids)
	it := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
	defer it.Stop()
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		inbox++
	}
	return dedupe, inbox
}

// A crash between the victim-delete commit and the trailing sweep/reap must
// stay recoverable: the durably-written marker lets a later purge resume the
// cleanup where the lost in-memory residual cannot, and an ID-reusing
// replacement must not consume the old rows.
func TestPurgeMarkerCrashResume(t *testing.T) {
	b, ctx := commitTerminateTestBackend(t)

	const id = "purge-marker-crash"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	crashPurgeAfterDelete(t, b, ctx, id)
	markerDocMustExist(t, b, ctx, id)

	// The later purge finds no victim (instance doc gone) but resumes via
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
	if msnap, err := b.ref(purgeMarkersCollection, id).Get(ctx); err == nil && msnap.Exists() {
		t.Fatal("marker must be cleared once the resumed cleanup completes")
	}

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
func TestPurgeMarkerReplacementDefersToReplacementPurge(t *testing.T) {
	b, ctx := commitTerminateTestBackend(t)

	const id = "purge-marker-replacement"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	v := crashPurgeAfterDelete(t, b, ctx, id)

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	n, err := b.PurgeInstances(ctx, 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("purge with live replacement: purged %d, want 0", n)
	}
	// Rows stay (ambiguous), marker stays (fresh).
	if dedupe, _ := childRowsFor(t, b, ctx, id); dedupe == 0 {
		t.Fatal("ambiguous straggler rows must stay for the replacement's own purge")
	}
	markerDocMustExist(t, b, ctx, id)

	// Backdate the marker past TTL: the next purge drops the metadata but
	// still preserves the rows.
	_, err = b.ref(purgeMarkersCollection, id).Set(ctx, map[string]any{
		"instance_id": id,
		"created_at":  v.createdAt,
		"purged_at":   time.Now().UTC().Add(-purgeMarkerTTL - time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := b.PurgeInstances(ctx, 0, nil, 10); err != nil || n != 0 {
		t.Fatalf("stale-marker purge: n=%d err=%v, want 0 nil", n, err)
	}
	if msnap, err := b.ref(purgeMarkersCollection, id).Get(ctx); err == nil && msnap.Exists() {
		t.Fatal("stale marker with a live replacement must age out")
	}
	if dedupe, _ := childRowsFor(t, b, ctx, id); dedupe == 0 {
		t.Fatal("TTL expiry must drop only the marker, never the ambiguous rows")
	}
	// Replacement still fully functional.
	if _, err := b.GetInstance(ctx, id); err != nil {
		t.Fatalf("replacement must survive: %v", err)
	}
}
