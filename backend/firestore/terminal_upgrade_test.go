package firestore

import (
	"context"
	"os"
	"testing"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Upgrading with an already-terminal instance that received a deduplicated
// post-terminal signal under the previous release duplicates it once on the
// first retry (Codex round-15 on #296): the old release stored the retry
// guard only in wf_signal_dedupe (a legacy unversioned row), so the marker
// collection is empty and the marker probe misses. The terminal send inserts
// its event (a doomed pre-terminal row must never suppress a terminal send —
// see TestTerminalWindowSendDeliversExactlyOnce) while stamping the marker,
// so only the first post-upgrade retry duplicates; every later retry takes
// the marker fast path. Without the round-15 fix the first retry inserts
// nothing and the signal is lost permanently once the sweep removes the row.
func TestTerminalUpgradeLegacyGuardDeliversOnce(t *testing.T) {
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

	const id = "terminal-upgrade-guard"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// The instance is already terminal at upgrade time (old release
	// deleted every pre-terminal dedupe key at the terminal commit).
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Seed exactly what the previous release wrote for a post-terminal
	// send: a legacy guard row with NO format_version field, plus the
	// delivered inbox event.
	if _, err := b.ref("wf_signal_dedupe", id+":k-upg").Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   "k-upg",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	inboxRow := newID()
	if _, err := b.ref("wf_inbox", inboxID(id, inboxRow)).Create(ctx, inboxDoc(id, inboxRow, 1, ev, nowUTC())); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("setup inbox=%d want 1", n)
	}
	// First post-upgrade retry: inserts (one duplicate) and stamps the
	// marker for next time.
	if err := b.SendToInbox(ctx, id, ev, "k-upg"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("post-upgrade retry must insert alongside the legacy guard: inbox=%d want 2", n)
	}
	msnap, err := b.ref(postTerminalMarkersCollection, postTerminalMarkerDocID(id, "k-upg")).Get(ctx)
	if err != nil || !msnap.Exists() {
		t.Fatalf("terminal send did not stamp the marker: snap=%v err=%v", msnap, err)
	}
	// Second retry (marker hit): still two events, no further duplicate.
	if err := b.SendToInbox(ctx, id, ev, "k-upg"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("marker retry duplicated the signal: inbox=%d want 2", n)
	}
	// Reset semantics stay intact: a fresh DedupeID on the same terminal
	// instance still delivers.
	if err := b.SendToInbox(ctx, id, ev, "k-new"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 3 {
		t.Fatalf("fresh terminal send lost: inbox=%d want 3", n)
	}
}

// A send that commits in the notify-to-sweep window — after the terminal
// transaction committed and notified, but before the post-commit sweep
// deletes the snapshotted pre-terminal legacy row — must deliver its event
// immediately (Codex round-15 on #296). Suppressing it on the doomed row
// while stamping only a marker loses the signal permanently: the sweep
// removes the row and the marker then suppresses every retry.
func TestTerminalWindowSendDeliversExactlyOnce(t *testing.T) {
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

	const id = "terminal-window-send"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Freeze the window: terminal status committed, but the pre-terminal
	// legacy row the sweep will delete is still present and no marker
	// exists yet.
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ref("wf_signal_dedupe", id+":k-win").Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   "k-win",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 0 {
		t.Fatalf("setup inbox=%d want 0", n)
	}
	// The window send must INSERT, not suppress-then-sweep.
	if err := b.SendToInbox(ctx, id, ev, "k-win"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("window send lost: inbox=%d want 1", n)
	}
	// The post-commit sweep now removes the doomed pre-terminal row.
	if _, err := b.ref("wf_signal_dedupe", id+":k-win").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	// The retry must dedupe via the marker, not re-insert.
	if err := b.SendToInbox(ctx, id, ev, "k-win"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("marker retry duplicated the window send: inbox=%d want 1", n)
	}
}
