package spanner

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// A SendToInbox that commits while the instance is already terminal must not
// be swallowed by a pre-terminal dedupe key (Codex round 4 on #327): the key
// may have been snapshotted for the post-commit sweep, so treating the send
// as a duplicate and then sweeping the key loses the signal. The first
// terminal send always inserts its event, but retries still dedupe via a
// post-terminal marker (Codex round 5 on #327): every retry with an existing
// marker must not insert another event.
func TestTerminalSendBypassesStaleDedupe(t *testing.T) {
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
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id": id, "status": "terminated",
				"updated_at": nowUTC(), "completed_at": nowUTC(),
			}),
		})
	})
	if err != nil {
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
// snapshotted before the victim delete must still be reaped by exact key
// (Codex round 5 on #327): comparing created_at wall clocks across processes
// misclassifies a replacement from a behind-clock node as older.
func TestPurgeReapsReplacedStragglers(t *testing.T) {
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

	const id = "purge-straggler-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Seed a straggler via the public API, then snapshot it by exact key
	// before the victim delete — the snapshot, not wall-clock comparison,
	// proves it is old.
	evOld := journal.Event{Type: journal.TypeSignalReceived, Name: "old-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, evOld, "old-key"); err != nil {
		t.Fatal(err)
	}
	residual, err := b.listResidualStragglers(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(residual.dedupe)+len(residual.inboxIDs) == 0 {
		t.Fatal("residual snapshot is empty; nothing to reap")
	}
	// Simulate the purge victim delete followed by an ID-reusing
	// CreateInstance: drop the instance row with its first-sweep children
	// (journal row and workflow task), recreate, then add the replacement's
	// own signal (which must survive the reap).
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		iter := txn.Query(ctx, spanner.Statement{
			SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id`,
			Params: map[string]any{"id": id},
		})
		defer iter.Stop()
		var muts []*spanner.Mutation
		muts = append(muts,
			spanner.Delete("wf_instances", spanner.Key{id}),
			spanner.Delete("wf_journal", spanner.Key{id, int64(1)}),
		)
		for {
			trow, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			var taskID int64
			if err := trow.Columns(&taskID); err != nil {
				return err
			}
			muts = append(muts, spanner.Delete("wf_tasks", spanner.Key{taskID}))
		}
		return txn.BufferWrite(muts)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "api-key"); err != nil {
		t.Fatal(err)
	}
	if err := b.reapResidualStragglers(ctx, id, residual); err != nil {
		t.Fatal(err)
	}
	if b.dedupeKeyExists(ctx, id, "old-key") {
		t.Fatal("old dedupe key survived the straggler reap")
	}
	// The replacement's own key (created after the snapshot) must survive.
	// Note: the replacement is running, so its send creates the base key
	// only, not a post-terminal marker.
	if !b.dedupeKeyExists(ctx, id, "api-key") {
		t.Fatal(`replacement dedupe key "api-key" was reaped`)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Only the replacement row may remain after the reap.
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d after reap, want 1 (old row leaked or new row reaped)", len(st.Inbox))
	}
	if st.Inbox[0].Event.Name != "new-signal" {
		t.Fatalf("remaining inbox row %q is not the replacement's signal", st.Inbox[0].Event.Name)
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
// "__post_terminal__:x" occupies a different row than the marker for user
// ID "x". Without the escape, the terminal send of "x" below would see the
// pre-terminal row, mistake itself for a retry, and lose the signal.
func TestTerminalMarkerNamespaceCollision(t *testing.T) {
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
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id": id, "status": "terminated",
				"updated_at": nowUTC(), "completed_at": nowUTC(),
			}),
		})
	})
	if err != nil {
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

	const id = "terminal-legacy-user-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Pre-upgrade user key stored verbatim (no "__" escape): the exact row
	// the round-8 marker probe for "x" used to hit.
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_signal_dedupe", map[string]any{
				"instance_id": id, "dedupe_id": "__post_terminal__:x", "created_at": nowUTC(),
			}),
		})
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Terminal transition with the legacy row still present.
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id": id, "status": "terminated",
				"updated_at": nowUTC(), "completed_at": nowUTC(),
			}),
		})
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
// of "__post_terminal__v1:x" occupies the very row the v1 marker probe for
// "x" reads, and prefixes cannot separate them. Markers now live outside
// the dedupe keyspace, so the probe never consults wf_signal_dedupe. The
// legacy row is seeded directly to bypass the escaped write path. On the old
// in-dedupe code the terminal send is swallowed and this fails.
func TestTerminalLegacyV1UserRowDelivers(t *testing.T) {
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

	const id = "terminal-legacy-v1-user-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Legacy user key stored verbatim (no "__" escape): the exact row the
	// old v1 marker probe for "x" used to hit.
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_signal_dedupe", map[string]any{
				"instance_id": id, "dedupe_id": "__post_terminal__v1:x", "created_at": nowUTC(),
			}),
		})
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	// Terminal transition with the legacy row still present.
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.UpdateMap("wf_instances", map[string]any{
				"id": id, "status": "terminated",
				"updated_at": nowUTC(), "completed_at": nowUTC(),
			}),
		})
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
	if _, merr := b.client.Single().ReadRow(ctx, postTerminalMarkersTable, spanner.Key{id, dedupeMarkerKey("x")}, []string{"marker_key"}); merr != nil {
		t.Fatalf("retry marker for %q must live in %s (err=%v)", "x", postTerminalMarkersTable, merr)
	}
}

// An inert pre-upgrade marker row left in wf_signal_dedupe must never match
// a user-key probe (Codex round 11 on #296): user probes skip
// marker-shaped candidates, so a non-terminal first send with a
// marker-shaped DedupeID inserts instead of deduping against the leftover
// marker. On the old code the send is swallowed and this fails.
func TestNonTerminalSendBypassesStaleMarkerRow(t *testing.T) {
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

	const id = "nonterminal-stale-marker-row"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Inert pre-upgrade marker row for "y", exactly as the old code wrote
	// it: the raw user-key probe for "__post_terminal__v1:y" used to hit it.
	if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_signal_dedupe", map[string]any{
				"instance_id": id, "dedupe_id": "__post_terminal__v1:y", "created_at": nowUTC(),
			}),
		})
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
// recreated after the snapshot (Codex round 6 on #327): dedupe keys are
// deterministic ((instance_id, dedupe_id)), so an unconditional exact-key
// delete would strip the replacement's live guard while its inbox event
// remains, duplicating a later retry. A straggler the replacement never
// touched is still reaped.
func TestPurgePreservesRecreatedDedupeKey(t *testing.T) {
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

	const id = "purge-recreated-key"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	evOld := journal.Event{Type: journal.TypeSignalReceived, Name: "old-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, evOld, "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, evOld, "straggler-key"); err != nil {
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
	// it creates a FRESH row (new created_at) with its inbox event — the
	// reap must recognize the version change and skip it.
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL:    `DELETE FROM wf_signal_dedupe WHERE instance_id = @id AND dedupe_id = @k`,
			Params: map[string]any{"id": id, "k": "old-key"},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		iter := txn.Query(ctx, spanner.Statement{
			SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id`,
			Params: map[string]any{"id": id},
		})
		defer iter.Stop()
		var muts []*spanner.Mutation
		muts = append(muts,
			spanner.Delete("wf_instances", spanner.Key{id}),
			spanner.Delete("wf_journal", spanner.Key{id, int64(1)}),
		)
		for {
			trow, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			var taskID int64
			if err := trow.Columns(&taskID); err != nil {
				return err
			}
			muts = append(muts, spanner.Delete("wf_tasks", spanner.Key{taskID}))
		}
		return txn.BufferWrite(muts)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "old-key"); err != nil {
		t.Fatal(err)
	}
	if err := b.reapResidualStragglers(ctx, id, residual); err != nil {
		t.Fatal(err)
	}
	// The replacement's recreated key survives; the untouched straggler is
	// still reaped (the version guard skips only changed rows).
	if !b.dedupeKeyExists(ctx, id, "old-key") {
		t.Fatal("replacement dedupe key was reaped; a later retry would duplicate the signal")
	}
	if b.dedupeKeyExists(ctx, id, "straggler-key") {
		t.Fatal("untouched straggler key survived the reap")
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 || st.Inbox[0].Event.Name != "new-signal" {
		t.Fatalf("inbox after reap holds %d rows, want only the replacement's new-signal", len(st.Inbox))
	}
}
