package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestSignalBatch_InsertsAll(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sb-1", Name: "WF", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	c := client.NewClient(b)
	err := c.SignalBatch(ctx, "sb-1", []client.SignalItem{
		{Name: "a", Payload: map[string]int{"n": 1}},
		{Name: "b", Payload: map[string]int{"n": 2}, DedupeID: "d1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d", len(st.Inbox))
	}
	if st.Inbox[0].Event.Type != journal.TypeSignalReceived || st.Inbox[0].Event.Name != "a" {
		t.Fatalf("%+v", st.Inbox[0].Event)
	}
}

func TestSignalBatch_DedupeSkip(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sb-2", Name: "WF", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	c := client.NewClient(b)
	if err := c.SignalBatch(ctx, "sb-2", []client.SignalItem{
		{Name: "a", Payload: 1, DedupeID: "x"},
		{Name: "b", Payload: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SignalBatch(ctx, "sb-2", []client.SignalItem{
		{Name: "a", Payload: 9, DedupeID: "x"},
		{Name: "c", Payload: 3},
	}); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "sb-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 3 {
		t.Fatalf("inbox=%d want 3 (second a skipped)", len(st.Inbox))
	}
}

func TestSignalBatch_EmptyAndNotFound(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	c := client.NewClient(b)
	if err := c.SignalBatch(ctx, "missing", nil); err != nil {
		t.Fatal(err)
	}
	err := c.SignalBatch(ctx, "missing", []client.SignalItem{{Name: "a", Payload: 1}})
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestSignalBatch_TooLarge(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sb-big", Name: "WF", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	items := make([]client.SignalItem, backend.DefaultInboxBatchLimit+1)
	for i := range items {
		items[i] = client.SignalItem{Name: "n", Payload: i}
	}
	err := client.NewClient(b).SignalBatch(ctx, "sb-big", items)
	if !errors.Is(err, backend.ErrBatchTooLarge) {
		t.Fatalf("err=%v", err)
	}
}
