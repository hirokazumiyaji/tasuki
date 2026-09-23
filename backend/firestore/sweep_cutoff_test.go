package firestore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// The post-commit dedupe sweep must preserve keys created after the terminal
// transition (Codex round 2 on #327). notifyTerminal fires before the sweep,
// so a concurrent SendToInbox with a new DedupeID can land inside the sweep
// window; deleting its key while the inbox event remains would duplicate a
// later retry of the same DedupeID.
func TestSweepSignalDedupePreservesPostCommitKeys(t *testing.T) {
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

	const id = "dedupe-cutoff"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	commit := time.Now().UTC()
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
	put("pre-commit", commit.Add(-time.Second))
	put("post-commit", commit.Add(time.Second))

	if err := b.sweepSignalDedupe(ctx, id, commit); err != nil {
		t.Fatal(err)
	}
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "pre-commit")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("pre-commit dedupe key survived the sweep")
	}
	snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "post-commit")).Get(ctx)
	if err != nil || !snap.Exists() {
		t.Fatalf("post-commit dedupe key was swept (err=%v); a later retry would duplicate the signal", err)
	}
}
