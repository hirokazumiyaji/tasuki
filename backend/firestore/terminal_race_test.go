package firestore

import (
	"context"
	"os"
	"testing"
	"time"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// A SendToInbox that commits while the instance is already terminal must not
// be swallowed by a pre-terminal dedupe key (Codex round 4 on #327): the key
// may have been snapshotted for the post-commit sweep, so treating the send
// as a duplicate and then sweeping the key loses the signal. Terminal sends
// always insert their event.
func TestTerminalSendBypassesStaleDedupe(t *testing.T) {
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

	const id = "terminal-send-bypass"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "k-race"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("setup inbox=%d want 1", n)
	}
	// Simulate the race window: the terminal transition committed while the
	// pre-terminal dedupe key is still present (post-commit sweep pending).
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, ev, "k-race"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("terminal resend with a stale dedupe key: inbox=%d want 2 (signal lost)", n)
	}
}

// When the purge second sweep stops at a replacement incarnation, rows that
// provably predate the replacement must still be reaped (Codex round 4 on
// #327): an inbox row committed between the first sweep and the victim
// delete would otherwise be consumed by the replacement's LoadWorkflow.
func TestPurgeReapsReplacedStragglers(t *testing.T) {
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

	const id = "purge-straggler-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Simulate the purge victim delete followed by an ID-reusing
	// CreateInstance: drop the instance doc with its first-sweep children
	// (journal row and workflow task, as the guarded first sweep leaves
	// them), recreate, and read back the replacement's incarnation marker.
	if _, err := b.ref("wf_instances", id).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ref("wf_journal", journalID(id, 1)).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ref("wf_tasks", wfTaskID(id)).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	rsnap, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replCreated := timestamp(rsnap.Data(), "created_at")

	seedDedupe := func(dedupeID string, createdAt time.Time) {
		t.Helper()
		_, err := b.ref("wf_signal_dedupe", signalDedupeID(id, dedupeID)).Create(ctx, map[string]any{
			"instance_id": id, "dedupe_id": dedupeID, "created_at": createdAt,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	seedInbox := func(name string, createdAt time.Time) {
		t.Helper()
		docID := newID()
		_, err := b.ref("wf_inbox", inboxID(id, docID)).Create(ctx,
			inboxDoc(id, docID, 1, journal.Event{Type: journal.TypeSignalReceived, Name: name, Payload: []byte(`{}`)}, createdAt))
		if err != nil {
			t.Fatal(err)
		}
	}
	oldTs := replCreated.Add(-2 * time.Second)
	seedDedupe("old-key", oldTs)
	seedInbox("old-signal", oldTs)
	// The replacement's own rows must survive: one stamped exactly at the
	// incarnation marker (tie goes to preservation) and one sent normally.
	seedDedupe("new-key", replCreated)
	seedInbox("tie-signal", replCreated)
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "api-key"); err != nil {
		t.Fatal(err)
	}

	if err := b.reapReplacedStragglers(ctx, id); err != nil {
		t.Fatal(err)
	}
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "old-key")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("old dedupe key survived the straggler reap")
	}
	for _, key := range []string{"new-key", "api-key"} {
		snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, key)).Get(ctx)
		if err != nil || !snap.Exists() {
			t.Fatalf("replacement dedupe key %q was reaped (err=%v)", key, err)
		}
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, item := range st.Inbox {
		names[item.Event.Name] = true
	}
	if names["old-signal"] {
		t.Fatal("old inbox row survived the straggler reap; the replacement would consume it")
	}
	for _, want := range []string{"tie-signal", "new-signal"} {
		if !names[want] {
			t.Fatalf("replacement inbox row %q was reaped", want)
		}
	}
}

func terminalTestInboxLen(t *testing.T, b *Backend, ctx context.Context, id string) int {
	t.Helper()
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return len(st.Inbox)
}
