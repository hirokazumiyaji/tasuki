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
// post-terminal signal under the previous release must not duplicate it on
// retry (Codex round 14 on #296): the old release stored the retry guard
// only in wf_signal_dedupe (a legacy unversioned row), so the marker
// collection is empty and the marker probe misses. The terminal base-key
// probe must recognize the legacy owned guard and suppress the retry (no
// new event) while stamping the marker for next time. Without the fix the
// first post-upgrade retry inserts a second inbox event.
func TestTerminalUpgradeLegacyGuardSuppressesRetry(t *testing.T) {
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
	// First post-upgrade retry: suppressed, no duplicate event.
	if err := b.SendToInbox(ctx, id, ev, "k-upg"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("post-upgrade retry duplicated the signal: inbox=%d want 1", n)
	}
	// The suppress path stamps the marker so the next retry takes the
	// marker fast path.
	msnap, err := b.ref(postTerminalMarkersCollection, postTerminalMarkerDocID(id, "k-upg")).Get(ctx)
	if err != nil || !msnap.Exists() {
		t.Fatalf("suppress path did not stamp the marker: snap=%v err=%v", msnap, err)
	}
	// Second retry (marker hit): still a single event.
	if err := b.SendToInbox(ctx, id, ev, "k-upg"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("marker retry duplicated the signal: inbox=%d want 1", n)
	}
	// Reset semantics stay intact: a fresh DedupeID on the same terminal
	// instance still delivers.
	if err := b.SendToInbox(ctx, id, ev, "k-new"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("fresh terminal send lost: inbox=%d want 2", n)
	}
}
