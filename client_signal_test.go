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

func TestClient_SignalDedupe(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "dedupe-1", Name: "WF", Queue: "default"})
	c := tasuki.NewClient(b)

	if err := c.Signal(ctx, "dedupe-1", "approve", map[string]string{"n": "1"}, tasuki.WithDedupeID("pay-1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Signal(ctx, "dedupe-1", "approve", map[string]string{"n": "2"}, tasuki.WithDedupeID("pay-1")); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "dedupe-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d want 1", len(st.Inbox))
	}

	if err := c.Signal(ctx, "dedupe-1", "approve", map[string]string{"n": "3"}, tasuki.WithDedupeID("pay-2")); err != nil {
		t.Fatal(err)
	}
	st, _ = b.LoadWorkflow(ctx, "dedupe-1")
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d want 2", len(st.Inbox))
	}

	// Without dedupe, duplicates are still delivered.
	if err := c.Signal(ctx, "dedupe-1", "approve", map[string]string{"n": "4"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Signal(ctx, "dedupe-1", "approve", map[string]string{"n": "5"}); err != nil {
		t.Fatal(err)
	}
	st, _ = b.LoadWorkflow(ctx, "dedupe-1")
	if len(st.Inbox) != 4 {
		t.Fatalf("inbox=%d want 4", len(st.Inbox))
	}
}

func TestClient_SignalDedupeClearedOnTerminate(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "dedupe-term", Name: "WF", Queue: "default"})
	c := tasuki.NewClient(b)

	if err := c.Signal(ctx, "dedupe-term", "approve", struct{}{}, tasuki.WithDedupeID("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.Terminate(ctx, "dedupe-term"); err != nil {
		t.Fatal(err)
	}
	// After terminal cleanup, the same dedupe ID may insert again.
	if err := c.Signal(ctx, "dedupe-term", "approve", struct{}{}, tasuki.WithDedupeID("x")); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "dedupe-term")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d want 2 after terminate+resignal", len(st.Inbox))
	}
}
