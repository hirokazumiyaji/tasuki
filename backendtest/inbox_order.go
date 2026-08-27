package backendtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// testInboxOrder verifies that LoadWorkflowHead returns inbox items in
// arrival order across sequential and batch sends.
func testInboxOrder(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("inbox-order-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("sig-%d", i)
		if err := b.SendToInbox(ctx, id, signal(name), ""); err != nil {
			t.Fatal(err)
		}
		want = append(want, name)
	}
	batch := []backend.InboxItem{}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("batch-%d", i)
		batch = append(batch, backend.InboxItem{Event: signal(name)})
		want = append(want, name)
	}
	if err := b.SendToInboxBatch(ctx, id, batch); err != nil {
		t.Fatal(err)
	}

	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(st.Inbox))
	for _, item := range st.Inbox {
		if item.Event.Type != journal.TypeSignalReceived {
			t.Fatalf("unexpected inbox event type %s", item.Event.Type)
		}
		got = append(got, item.Event.Name)
	}
	if len(got) != len(want) {
		t.Fatalf("inbox=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("inbox order %v want %v", got, want)
		}
	}
}

func signal(name string) journal.Event {
	return journal.Event{Type: journal.TypeSignalReceived, Name: name, Payload: []byte(`{}`)}
}
