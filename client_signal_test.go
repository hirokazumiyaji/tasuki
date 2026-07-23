package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestClient_SignalCreatesInbox(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "sig-1", Name: "WF", Queue: "default"})
	c := tasuki.NewClient(b)
	if err := c.Signal(ctx, "sig-1", "approve", map[string]string{"ok": "1"}); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "sig-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 || st.Inbox[0].Event.Type != journal.TypeSignalReceived || st.Inbox[0].Event.Name != "approve" {
		t.Fatalf("%+v", st.Inbox)
	}
}

func TestClient_CancelCreatesInbox(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "can-1", Name: "WF", Queue: "default"})
	c := tasuki.NewClient(b)
	if err := c.Cancel(ctx, "can-1"); err != nil {
		t.Fatal(err)
	}
	st, _ := b.LoadWorkflow(ctx, "can-1")
	if len(st.Inbox) != 1 || st.Inbox[0].Event.Type != journal.TypeCancelRequested {
		t.Fatalf("%+v", st.Inbox)
	}
}
