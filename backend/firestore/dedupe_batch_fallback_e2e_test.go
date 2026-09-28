package firestore

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// inboxCount counts inbox rows for one instance.
func inboxCount(t *testing.T, b *Backend, ctx context.Context, instanceID string) int {
	t.Helper()
	it := b.col("wf_inbox").Where("instance_id", "==", instanceID).Documents(ctx)
	defer it.Stop()
	n := 0
	for {
		_, err := it.Next()
		if err != nil {
			break
		}
		n++
	}
	return n
}

func dedupeSignal(id string) backend.InboxItem {
	return backend.InboxItem{
		Event:    journal.Event{Type: journal.TypeSignalReceived, Name: "sig"},
		DedupeID: id,
	}
}

// batchFallbackCleanup removes every row these tests own (scoped by
// instance ID — never a full Reset, so concurrent suites sharing the
// emulator are undisturbed) so the tests are re-runnable.
func batchFallbackCleanup(t *testing.T, b *Backend, ctx context.Context, instanceID string, keys []string) {
	t.Helper()
	del := func(col, id string) {
		_, _ = b.ref(col, id).Delete(ctx)
	}
	for _, k := range keys {
		for _, docID := range dedupeDocIDs(instanceID, []string{k}) {
			del("wf_signal_dedupe", docID)
		}
		del(postTerminalMarkersCollection, postTerminalMarkerDocID(instanceID, k))
	}
	it := b.col("wf_inbox").Where("instance_id", "==", instanceID).Documents(ctx)
	for {
		d, err := it.Next()
		if err != nil {
			break
		}
		_, _ = d.Ref.Delete(ctx)
	}
	it.Stop()
	jit := b.col("wf_journal").Where("instance_id", "==", instanceID).Documents(ctx)
	for {
		d, err := jit.Next()
		if err != nil {
			break
		}
		_, _ = d.Ref.Delete(ctx)
	}
	jit.Stop()
	del("wf_inbox_seq", instanceID)
	del("wf_tasks", wfTaskID(instanceID))
	del("wf_instances", instanceID)
	// A stale purge marker blocks CreateInstance the same way a live row
	// does (see CreateInstance); nothing in these tests purges, but the
	// shared emulator may hold one from an interrupted suite.
	del(purgeMarkersCollection, instanceID)
}

// TestForeignFramedHitStillDedupes checks that a foreign legacy guard at the
// framed doc ID does not suppress the first send. The framed ID for
// "framed-a" and legacy ID for "8:framed-a" are both "8:framed-a:x".
func TestForeignFramedHitStillDedupes(t *testing.T) {
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
	if f, l := frameDedupeDocID("framed-a", "x"), legacyDedupeDocID("8:framed-a", "x"); f != l {
		t.Fatalf("test setup: framings must alias, got %q vs %q", f, l)
	}
	const inst = "framed-a"
	batchFallbackCleanup(t, b, ctx, inst, []string{"x"})
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: inst, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Seed the foreign legacy row directly. Only the dedupe doc is probed;
	// the foreign instance does not need to exist.
	if _, err := b.ref("wf_signal_dedupe", legacyDedupeDocID("8:framed-a", "x")).Create(ctx, map[string]any{
		"instance_id": "8:framed-a",
		"dedupe_id":   "x",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInboxBatch(ctx, inst, []backend.InboxItem{dedupeSignal("x")}); err != nil {
		t.Fatal(err)
	}
	if n := inboxCount(t, b, ctx, inst); n != 1 {
		t.Fatalf("inbox rows after first send = %d, want 1", n)
	}
	// The guard must live at the free legacy framing in canonical form.
	gsnap, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(inst, "x")).Get(ctx)
	if err != nil || !gsnap.Exists() {
		t.Fatalf("legacy-framed guard missing: %v", err)
	}
	if g := gsnap.Data(); str(g, "dedupe_id") != "x" || i64(g, dedupeFormatVersionField) != int64(dedupeFormatVersion) {
		t.Fatalf("legacy guard = %v, want canonical v1 form", g)
	}
	// Retry must dedupe via that guard.
	if err := b.SendToInboxBatch(ctx, inst, []backend.InboxItem{dedupeSignal("x")}); err != nil {
		t.Fatal(err)
	}
	if n := inboxCount(t, b, ctx, inst); n != 1 {
		t.Fatalf("inbox rows after retry = %d, want 1 (retry must dedupe)", n)
	}
}

// TestBatchFallbackKeysDistinct checks batch fallback key selection end to end:
// batch IDs "____x" + "__x" with foreign-occupied "______x". The first item
// falls back to "____x" — the second item's canonical probe cannot see the
// buffered Create, so without the batch reservation both choose the same
// doc and the whole batch fails deterministically on every retry.
func TestBatchFallbackKeysDistinct(t *testing.T) {
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
	const inst = "batch-fallback"
	keys := []string{"____x", "__x", "______x"}
	batchFallbackCleanup(t, b, ctx, inst, keys)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: inst, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Seed the foreign legacy row at the first item's canonical key.
	if _, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(inst, "______x")).Create(ctx, map[string]any{
		"instance_id": inst,
		"dedupe_id":   "______x",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	batch := []backend.InboxItem{dedupeSignal("____x"), dedupeSignal("__x")}
	if err := b.SendToInboxBatch(ctx, inst, batch); err != nil {
		t.Fatalf("batch with colliding fallback keys must commit: %v", err)
	}
	if n := inboxCount(t, b, ctx, inst); n != 2 {
		t.Fatalf("inbox rows after batch = %d, want 2", n)
	}
	// Retrying the batch must dedupe via the two distinct guards.
	if err := b.SendToInboxBatch(ctx, inst, batch); err != nil {
		t.Fatal(err)
	}
	if n := inboxCount(t, b, ctx, inst); n != 2 {
		t.Fatalf("inbox rows after batch retry = %d, want 2 (must dedupe)", n)
	}
}
