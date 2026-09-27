package firestore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// seedFenceTimer inserts one timer row for id directly, modeling a timer the
// (old or replacement) incarnation scheduled.
func seedFenceTimer(t *testing.T, b *Backend, ctx context.Context, id string, seq int64) {
	t.Helper()
	_, err := b.ref("wf_timers", journalID(id, seq)).Create(ctx, map[string]any{
		"instance_id": id, "seq": seq, "fire_at": nowUTC().Add(time.Hour), "created_at": nowUTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func instanceCreatedAt(t *testing.T, b *Backend, ctx context.Context, id string) time.Time {
	t.Helper()
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return timestamp(snap.Data(), "created_at")
}

// instanceVictim captures the fence identity the terminal commit observes:
// the pre-commit incarnation (created_at plus the unique token) for the
// post-commit sweep fence.
func instanceVictim(t *testing.T, b *Backend, ctx context.Context, id string) purgeVictim {
	t.Helper()
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return purgeVictim{id: id, createdAt: timestamp(snap.Data(), "created_at"), incarnation: str(snap.Data(), incarnationField)}
}

func fenceDocExists(t *testing.T, b *Backend, ctx context.Context, col, docID string) bool {
	t.Helper()
	snap, err := b.ref(col, docID).Get(ctx)
	if isNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return snap.Exists()
}

// TestTerminalSweepStaleFenceKeepsReplacement covers the Codex round-20 P1
// finding on #296: PurgeInstances(0, ...) deleting a terminal instance while
// its terminal sweep is paused, then CreateInstance reusing the ID, then the
// sweep resuming. The resumed sweep's page queries filter by instance_id
// only, so without an incarnation fence it deletes the replacement's
// workflow task and timers; the exact-key dedupe sweep likewise deletes the
// replacement's recreated guard (dedupe doc IDs are deterministic), stripping
// it while its inbox event remains so a later retry duplicates the signal.
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
	oldVictim := instanceVictim(t, b, ctx, id)
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("dedupe snapshot holds %d keys, want the single guard K", len(snapshot))
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
	// the replacement's rows. Dedupe doc IDs are deterministic, so the
	// resent guard K recreates the very same document the snapshot names.
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatalf("recreate after purge: %v", err)
	}
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	seedFenceTimer(t, b, ctx, id, 8)
	replacement := map[string]string{
		"task":   wfTaskID(id),
		"timer":  journalID(id, 8),
		"dedupe": signalDedupeID(id, "K"),
	}
	for kind, docID := range replacement {
		col := map[string]string{"task": "wf_tasks", "timer": "wf_timers", "dedupe": "wf_signal_dedupe"}[kind]
		if !fenceDocExists(t, b, ctx, col, docID) {
			t.Fatalf("setup: replacement %s doc %s/%s missing", kind, col, docID)
		}
	}

	// The paused sweep resumes with its stale pre-commit fence: it must
	// abort (nil) instead of deleting the replacement's rows.
	stale := purgeFence{victim: oldVictim}
	if err := b.sweepTerminateDocs(ctx, oldVictim, snapshot); err != nil {
		t.Fatalf("stale terminate sweep: %v (want fenced abort to nil)", err)
	}
	if err := b.sweepSignalDedupeIDs(ctx, stale, snapshot); err != nil {
		t.Fatalf("stale dedupe sweep: %v (want fenced abort to nil)", err)
	}
	for kind, docID := range replacement {
		col := map[string]string{"task": "wf_tasks", "timer": "wf_timers", "dedupe": "wf_signal_dedupe"}[kind]
		if !fenceDocExists(t, b, ctx, col, docID) {
			t.Fatalf("stale sweep deleted the replacement's %s doc %s/%s", kind, col, docID)
		}
	}
	if inst, err := b.GetInstance(ctx, id); err != nil || inst.Status != "running" {
		t.Fatalf("replacement instance: %v %#v (want running)", err, inst)
	}

	// Positive control: with the CURRENT incarnation the same sweep still
	// cleans up. Flip the replacement terminal (sweep paused again), sweep
	// with its own fence, and require every row gone.
	flipStatusWithoutSweep(t, b, ctx, id)
	curVictim := instanceVictim(t, b, ctx, id)
	curSnapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.sweepTerminateDocs(ctx, curVictim, curSnapshot); err != nil {
		t.Fatal(err)
	}
	for kind, docID := range replacement {
		col := map[string]string{"task": "wf_tasks", "timer": "wf_timers", "dedupe": "wf_signal_dedupe"}[kind]
		if fenceDocExists(t, b, ctx, col, docID) {
			t.Fatalf("current-incarnation sweep left %s doc %s/%s behind", kind, col, docID)
		}
	}
}
