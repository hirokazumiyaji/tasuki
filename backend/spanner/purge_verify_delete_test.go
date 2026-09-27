package spanner

import (
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// TestPurgeVerifyDeleteRemovesVictimAndChildren exercises the round-19 P2
// purge shape end to end: sweep, then ONE atomic verify-and-delete, with no
// trailing second sweep that could fail after the parent row is gone. A
// terminated instance carrying inbox/dedupe/journal/task children purges to
// count 1 with every child table empty, and a repeat purge finds nothing
// (idempotent, no parent-less residue to reselect).
func TestPurgeVerifyDeleteRemovesVictimAndChildren(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	const id = "purge-verify-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Children across tables: a deduped signal (dedupe + inbox rows) plus
	// the workflow-start journal row and the workflow task.
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "k1"); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	n, err := b.PurgeInstances(ctx, 0, nil, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 1 {
		t.Fatalf("purged = %d, want 1", n)
	}
	if _, err := b.GetInstance(ctx, id); err != backend.ErrNotFound {
		t.Fatalf("GetInstance = %v, want ErrNotFound (parent deleted)", err)
	}
	count := func(sql string) int64 {
		t.Helper()
		iter := b.client.Single().Query(ctx, spanner.Statement{
			SQL: sql, Params: map[string]any{"id": id},
		})
		defer iter.Stop()
		var total int64
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				return total
			}
			if err != nil {
				t.Fatal(err)
			}
			var c int64
			if err := row.Columns(&c); err != nil {
				t.Fatal(err)
			}
			total += c
		}
	}
	for table, key := range map[string]string{
		"wf_signal_dedupe": "instance_id",
		"wf_tasks":         "instance_id",
		"wf_timers":        "instance_id",
		"wf_inbox":         "instance_id",
		"wf_journal":       "instance_id",
		"wf_inbox_seq":     "instance_id",
	} {
		if got := count(`SELECT COUNT(*) FROM ` + table + ` WHERE ` + key + ` = @id`); got != 0 {
			t.Fatalf("%s rows for %q = %d, want 0 (verify-delete left residue)", table, id, got)
		}
	}
	// Repeat purge: nothing left, count 0, no error.
	n, err = b.PurgeInstances(ctx, 0, nil, 100)
	if err != nil {
		t.Fatalf("repeat purge: %v", err)
	}
	if n != 0 {
		t.Fatalf("repeat purged = %d, want 0", n)
	}
}
