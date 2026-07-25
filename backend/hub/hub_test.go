package hub_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/hub"
)

func TestSubscribe_WakesOnNotifyTasks(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := h.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.NotifyTasks()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected wake")
	}
}

func TestSubscribeTerminal_DeliversID(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := h.SubscribeTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.NotifyTerminal("i1")
	select {
	case got := <-ch:
		if got != "i1" {
			t.Fatalf("got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected terminal")
	}
}

func TestSubscribe_CancelStopsDelivery(t *testing.T) {
	h := hub.New()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := h.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	h.NotifyTasks()
	select {
	case <-ch:
		t.Fatal("unexpected wake after cancel")
	case <-time.After(50 * time.Millisecond):
	}
}
