package backendtest

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func testSignalDedupe(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("dedupe-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "approve", Payload: []byte(`{"n":1}`)}
	if err := b.SendToInbox(ctx, id, ev, "pay-1"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "approve", Payload: []byte(`{"n":2}`)}, "pay-1"); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d want 1", len(st.Inbox))
	}

	if err := b.SendToInbox(ctx, id, ev, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, ev, ""); err != nil {
		t.Fatal(err)
	}
	st, _ = b.LoadWorkflow(ctx, id)
	if len(st.Inbox) != 3 {
		t.Fatalf("inbox=%d want 3 without dedupe", len(st.Inbox))
	}

	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, ev, "pay-1"); err != nil {
		t.Fatal(err)
	}
	st, _ = b.LoadWorkflow(ctx, id)
	// Terminate clears the inbox along with the dedupe tracking, so the
	// re-send inserts exactly one row instead of being deduped.
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d want 1 after terminate clears dedupe+inbox", len(st.Inbox))
	}
}

// testSignalDedupeBatch verifies that duplicate DedupeIDs within one
// SendToInboxBatch are skipped (first wins) without failing the batch.
func testSignalDedupeBatch(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("dedupe-batch-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	err := b.SendToInboxBatch(ctx, id, []backend.InboxItem{
		{Event: journal.Event{Type: journal.TypeSignalReceived, Name: "a", Payload: []byte(`{"n":1}`)}, DedupeID: "pay-batch"},
		{Event: journal.Event{Type: journal.TypeSignalReceived, Name: "b", Payload: []byte(`{"n":2}`)}, DedupeID: "pay-batch"},
		{Event: journal.Event{Type: journal.TypeSignalReceived, Name: "c", Payload: []byte(`{"n":3}`)}, DedupeID: ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d want 2 (one deduped + one undeduped)", len(st.Inbox))
	}
	got := map[string]bool{}
	for _, item := range st.Inbox {
		got[item.Event.Name] = true
	}
	if !got["a"] || !got["c"] || got["b"] {
		t.Fatalf("inbox names=%v want {a,c} (first duplicate wins, b skipped)", got)
	}
}
