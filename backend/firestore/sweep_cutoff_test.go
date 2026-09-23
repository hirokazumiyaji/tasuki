package firestore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// The post-commit dedupe sweep must preserve keys first appearing after the
// pre-commit snapshot (Codex round 3 on #327). Classification follows
// transaction serialization order, not SendToInbox client timestamps: a send
// that captures created_at before the terminal commit, loses the race, and
// retry-commits after it must keep its key — deleting it while the inbox
// event remains would duplicate a later retry of the same DedupeID.
func TestSweepSignalDedupeSnapshotPreservesPostCommitKeys(t *testing.T) {
	guardTestEmulator(t)
	ctx := context.Background()
	b, err := New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
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

	const id = "dedupe-snapshot"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	put := func(dedupeID string, createdAt time.Time) {
		t.Helper()
		doc := map[string]any{
			"instance_id": id,
			"dedupe_id":   dedupeID,
			"created_at":  createdAt,
		}
		if _, err := b.ref("wf_signal_dedupe", signalDedupeID(id, dedupeID)).Create(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}
	put("pre-commit", time.Now().UTC().Add(-time.Second))

	// Snapshot the pre-commit set, exactly as CommitAdvancements does before
	// its terminal commit transaction.
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("snapshot holds %d keys, want the single pre-commit key", len(snapshot))
	}

	// A racing send landing after the snapshot — even one stamping a stale
	// pre-commit client timestamp after losing a transaction race — must be
	// preserved: its key was never in the committed set.
	put("post-commit", time.Now().UTC().Add(-time.Second))
	put("post-commit-fresh", time.Now().UTC().Add(time.Second))

	if err := b.sweepSignalDedupeIDs(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "pre-commit")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("pre-commit dedupe key survived the sweep")
	}
	for _, key := range []string{"post-commit", "post-commit-fresh"} {
		snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, key)).Get(ctx)
		if err != nil || !snap.Exists() {
			t.Fatalf("post-snapshot dedupe key %q was swept (err=%v); a later retry would duplicate the signal", key, err)
		}
	}
}
