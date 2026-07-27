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
	id := "dedupe-" + t.Name()
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
	if len(st.Inbox) != 4 {
		t.Fatalf("inbox=%d want 4 after terminate clears dedupe", len(st.Inbox))
	}
}
