package firestore

import (
	"context"
	"os"
	"testing"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// A SendToInbox that commits while the instance is already terminal must not
// be swallowed by a pre-terminal dedupe key (Codex round 4 on #327): the key
// may have been snapshotted for the post-commit sweep, so treating the send
// as a duplicate and then sweeping the key loses the signal. The first
// terminal send always inserts its event, but retries still dedupe via a
// post-terminal marker (Codex round 5 on #327).
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
	// Retry idempotency: a second identical post-terminal send must dedupe
	// via the marker, not insert a third event.
	if err := b.SendToInbox(ctx, id, ev, "k-race"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("terminal retry duplicated the signal: inbox=%d want 2 (idempotency lost)", n)
	}
}

// When the purge second sweep stops at a replacement incarnation, rows
// snapshotted before the victim delete must still be reaped by exact
// reference (Codex round 5 on #327): comparing created_at wall clocks across
// processes misclassifies a replacement from a behind-clock node as older.
// Stragglers are keyed by the pre-delete listing, not by time.
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
	// Seed stragglers that the first sweep missed (committed during the
	// sweep): an inbox row and dedupe key via the public API so they carry
	// real stamps, then snapshot them by exact reference before the victim
	// delete — the snapshot, not wall-clock comparison, proves they are old.
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "old-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "old-key"); err != nil {
		t.Fatal(err)
	}
	residual, err := b.listResidualStragglers(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(residual.dedupe)+len(residual.inbox) == 0 {
		t.Fatal("residual snapshot is empty; nothing to reap")
	}
	// Simulate the purge victim delete followed by an ID-reusing
	// CreateInstance: drop the instance doc with its first-sweep children
	// (journal row and workflow task), recreate, then add the replacement's
	// own rows (which must survive the reap despite any clock skew).
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
	evNew := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, evNew, "api-key"); err != nil {
		t.Fatal(err)
	}

	if err := b.reapResidualStragglers(ctx, residual); err != nil {
		t.Fatal(err)
	}
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "old-key")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("old dedupe key survived the straggler reap")
	}
	// The replacement's own key (created after the snapshot) must survive.
	// Note: the replacement is running, so its send creates the base key
	// only, not a post-terminal marker.
	snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "api-key")).Get(ctx)
	if err != nil || !snap.Exists() {
		t.Fatalf("replacement dedupe key %q was reaped (err=%v)", "api-key", err)
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
	if !names["new-signal"] {
		t.Fatal("replacement inbox row new-signal was reaped")
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

// A user DedupeID carrying the internal marker prefix must never collide
// with a post-terminal retry marker (Codex round 6 on #327): the marker
// namespace is escaped on write/read, so a pre-terminal send of
// "__post_terminal__:x" occupies a different document than the marker for
// user ID "x". Without the escape, the terminal send of "x" below would see
// the pre-terminal row, mistake itself for a retry, and lose the signal.
func TestTerminalMarkerNamespaceCollision(t *testing.T) {
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

	const id = "terminal-marker-collision"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Ordinary dedupe still works for the exotic ID while running.
	if err := b.SendToInbox(ctx, id, ev, "__post_terminal__:x"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, ev, "__post_terminal__:x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("exotic-ID resend duplicated the signal: inbox=%d want 1", n)
	}
	// Terminal transition with the pre-terminal key still present.
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	// The first post-terminal send of "x" must insert, not dedupe against
	// the pre-terminal "__post_terminal__:x" row.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("terminal send swallowed by marker collision: inbox=%d want 2 (signal lost)", n)
	}
	// Retry idempotency via the real marker still holds.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 2 {
		t.Fatalf("terminal retry duplicated the signal: inbox=%d want 2 (idempotency lost)", n)
	}
}

// A pre-upgrade verbatim user row ("__post_terminal__:x", stored before the
// round-6 escape) must never match the versioned retry-marker probe for "x"
// (Codex round 9 on #327): the round-8 dual-read mistook it for a marker and
// dropped the first post-terminal send of "x" (lost signal). The legacy row
// is seeded directly to bypass the escaped write path. On the old code the
// terminal send is swallowed and this fails.
func TestTerminalLegacyUserRowDelivers(t *testing.T) {
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

	const id = "terminal-legacy-user-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Pre-upgrade user key stored verbatim (no "__" escape): the exact row
	// the round-8 marker probe for "x" used to hit.
	if _, err := b.ref("wf_signal_dedupe", id+":"+"__post_terminal__:x").Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   "__post_terminal__:x",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Terminal transition with the legacy row still present.
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	// The first post-terminal send of "x" must insert, not dedupe against
	// the legacy "__post_terminal__:x" user row.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("terminal send swallowed by legacy user row: inbox=%d want 1 (signal lost)", n)
	}
	// Retry idempotency via the versioned marker still holds.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("terminal retry duplicated the signal: inbox=%d want 1 (idempotency lost)", n)
	}
}

// A legacy verbatim user row shaped like a VERSIONED marker
// ("__post_terminal__v1:x") must never match the retry-marker probe for "x"
// (Codex round 11 on #296): old code stored DedupeIDs verbatim, so a user ID
// of "__post_terminal__v1:x" occupies the very document the v1 marker probe
// for "x" reads, and prefixes cannot separate them. Markers now live outside
// the dedupe keyspace, so the probe never consults wf_signal_dedupe. The
// legacy row is seeded directly to bypass the escaped write path. On the old
// in-dedupe code the terminal send is swallowed and this fails.
func TestTerminalLegacyV1UserRowDelivers(t *testing.T) {
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

	const id = "terminal-legacy-v1-user-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Legacy user key stored verbatim (no "__" escape): the exact document
	// the old v1 marker probe for "x" used to hit.
	if _, err := b.ref("wf_signal_dedupe", id+":"+"__post_terminal__v1:x").Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   "__post_terminal__v1:x",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Terminal transition with the legacy row still present.
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{
		{Path: "status", Value: "terminated"},
		{Path: "completed_at", Value: nowUTC()},
	}); err != nil {
		t.Fatal(err)
	}
	// The first post-terminal send of "x" must insert, not dedupe against
	// the legacy "__post_terminal__v1:x" user row.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("terminal send swallowed by legacy v1 user row: inbox=%d want 1 (signal lost)", n)
	}
	// Retry idempotency via the moved marker still holds, and the marker
	// lives outside the dedupe keyspace.
	if err := b.SendToInbox(ctx, id, ev, "x"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("terminal retry duplicated the signal: inbox=%d want 1 (idempotency lost)", n)
	}
	msnap, merr := b.ref(postTerminalMarkersCollection, postTerminalMarkerDocID(id, "x")).Get(ctx)
	if merr != nil || !msnap.Exists() {
		t.Fatalf("retry marker for %q must live in %s (err=%v)", "x", postTerminalMarkersCollection, merr)
	}
}

// An inert pre-upgrade marker row left in wf_signal_dedupe must never match
// a user-key probe (Codex round 11 on #296): user probes skip
// marker-shaped candidates, so a non-terminal first send with a
// marker-shaped DedupeID inserts instead of deduping against the leftover
// marker. On the old code the send is swallowed and this fails.
func TestNonTerminalSendBypassesStaleMarkerRow(t *testing.T) {
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

	const id = "nonterminal-stale-marker-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Inert pre-upgrade marker row for "y", exactly as the old code wrote
	// it: the raw user-key probe for "__post_terminal__v1:y" used to hit it.
	if _, err := b.ref("wf_signal_dedupe", id+":"+"__post_terminal__v1:y").Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   "__post_terminal__v1:y",
		"created_at":  nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// First send with the marker-shaped DedupeID must insert under its
	// escaped user key, not dedupe against the stale marker row.
	if err := b.SendToInbox(ctx, id, ev, "__post_terminal__v1:y"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("send swallowed by stale marker row: inbox=%d want 1 (signal lost)", n)
	}
	// Ordinary dedupe still works for the exotic ID.
	if err := b.SendToInbox(ctx, id, ev, "__post_terminal__v1:y"); err != nil {
		t.Fatal(err)
	}
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("exotic-ID resend duplicated the signal: inbox=%d want 1", n)
	}
}

// When the purge second sweep stops at a replacement incarnation, the
// version-conditioned reap must preserve a dedupe key the replacement
// recreated after the snapshot (Codex round 6 on #327): dedupe document IDs
// are deterministic, so exact-reference deletion would strip the
// replacement's live guard while its inbox event remains, duplicating a
// later retry. A straggler the replacement never touched is still reaped.
func TestPurgePreservesRecreatedDedupeKey(t *testing.T) {
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

	const id = "purge-recreated-key"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "old-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, ev, "straggler-key"); err != nil {
		t.Fatal(err)
	}
	residual, err := b.listResidualStragglers(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(residual.dedupe) != 2 {
		t.Fatalf("residual dedupe snapshot holds %d keys, want 2", len(residual.dedupe))
	}
	// Simulate a concurrent sweep deleting the straggler row after the
	// snapshot, then the purge victim delete and an ID-reusing
	// CreateInstance. The replacement's send of "old-key" finds no row, so
	// it creates a FRESH document (new server update time) with its inbox
	// event — the reap must recognize the version change and skip it.
	if _, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "old-key")).Delete(ctx); err != nil {
		t.Fatal(err)
	}
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
	evNew := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, evNew, "old-key"); err != nil {
		t.Fatal(err)
	}

	if err := b.reapResidualStragglers(ctx, residual); err != nil {
		t.Fatal(err)
	}
	// The replacement's recreated key survives; the untouched straggler is
	// still reaped (the version guard skips only changed rows).
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "old-key")).Get(ctx); err != nil || !snap.Exists() {
		t.Fatalf("replacement dedupe key was reaped (err=%v); a later retry would duplicate the signal", err)
	}
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "straggler-key")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("untouched straggler key survived the reap")
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 || st.Inbox[0].Event.Name != "new-signal" {
		t.Fatalf("inbox after reap = %v, want only the replacement's new-signal", st.Inbox)
	}
}
