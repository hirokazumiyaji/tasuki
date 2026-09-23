package spanner

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Upgrading with an already-terminal instance that received a deduplicated
// post-terminal signal under the previous release must not duplicate it on
// retry (Codex round 14 on #296): the old release stored the retry guard
// only in wf_signal_dedupe (a legacy NULL-format_version row), so the marker
// table is empty and the marker probe misses. The terminal base-key probe
// must recognize the legacy owned guard and suppress the retry (no new
// event) while stamping the marker for next time. Without the fix the first
// post-upgrade retry inserts a second inbox event.
func TestTerminalUpgradeLegacyGuardSuppressesRetry(t *testing.T) {
	dsn := guardTestDSN(t)
	ctx := context.Background()
	b, err := New(ctx, dsn)
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
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// The instance is already terminal at upgrade time (the old release
	// deleted every pre-terminal dedupe key at the terminal commit).
	// Seed exactly what the previous release wrote for a post-terminal
	// send: a legacy guard row with NULL format_version, plus the
	// delivered inbox event.
	now := nowUTC()
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id": id, "status": "terminated",
				"updated_at": now, "completed_at": now,
			}),
			spanner.InsertMap("wf_signal_dedupe", map[string]any{
				"instance_id": id, "dedupe_id": "k-upg", "created_at": now,
			}),
			spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": id, "seq": int64(1),
				"type": string(ev.Type), "ref_seq": nullInt(ev.RefSeq),
				"payload": jsonVal(inboxPayload(ev)), "created_at": now,
			}),
		})
	}); err != nil {
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
	if _, merr := b.client.Single().ReadRow(ctx, postTerminalMarkersTable, spanner.Key{id, dedupeMarkerKey("k-upg")}, []string{"marker_key"}); merr != nil {
		t.Fatalf("suppress path did not stamp the marker: %v", merr)
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
