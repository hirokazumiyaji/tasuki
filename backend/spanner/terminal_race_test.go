package spanner

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// A SendToInbox that commits while the instance is already terminal must not
// be swallowed by a pre-terminal dedupe key (Codex round 4 on #327): the key
// may have been snapshotted for the post-commit sweep, so treating the send
// as a duplicate and then sweeping the key loses the signal. Terminal sends
// always insert their event.
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
}

// When the purge second sweep stops at a replacement incarnation, rows that
// provably predate the replacement must still be reaped (Codex round 4 on
// #327): an inbox row committed between the first sweep and the victim
// delete would otherwise be consumed by the replacement's LoadWorkflow.
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
	// Simulate the purge victim delete followed by an ID-reusing
	// CreateInstance: drop the instance row with its first-sweep children
	// (journal row and workflow task, as the guarded first sweep leaves
	// them), recreate, and read back the replacement's incarnation marker.
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
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at"})
	if err != nil {
		t.Fatal(err)
	}
	var replCreated time.Time
	if err := row.Columns(&replCreated); err != nil {
		t.Fatal(err)
	}

	seed := func(muts ...*spanner.Mutation) {
		t.Helper()
		_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite(muts)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	oldTs := replCreated.Add(-2 * time.Second)
	oldPayload := jsonVal(inboxPayload(journal.Event{Type: journal.TypeSignalReceived, Name: "old-signal", Payload: []byte(`{}`)}))
	seed(
		spanner.InsertMap("wf_signal_dedupe", map[string]any{
			"instance_id": id, "dedupe_id": "old-key", "created_at": oldTs,
		}),
		spanner.InsertMap("wf_inbox", map[string]any{
			"id": newID(), "instance_id": id, "seq": int64(1),
			"type": string(journal.TypeSignalReceived), "ref_seq": nullInt(0),
			"payload": oldPayload, "created_at": oldTs,
		}),
		// The replacement's own rows must survive: one stamped exactly at
		// the incarnation marker (tie goes to preservation).
		spanner.InsertMap("wf_signal_dedupe", map[string]any{
			"instance_id": id, "dedupe_id": "new-key", "created_at": replCreated,
		}),
	)
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "new-signal", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "api-key"); err != nil {
		t.Fatal(err)
	}
	if err := b.reapReplacedStragglers(ctx, id); err != nil {
		t.Fatal(err)
	}
	if b.dedupeKeyExists(ctx, id, "old-key") {
		t.Fatal("old dedupe key survived the straggler reap")
	}
	for _, key := range []string{"new-key", "api-key"} {
		if !b.dedupeKeyExists(ctx, id, key) {
			t.Fatalf("replacement dedupe key %q was reaped", key)
		}
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Old row (1) + replacement API row (1) = 2 before the reap; only the
	// replacement row may remain after it.
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
