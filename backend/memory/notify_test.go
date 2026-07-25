package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: "notify-wake-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected notify after CreateInstance")
	}
}

func TestSubscribeTerminal_WakesOnTerminate(t *testing.T) {
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := b.SubscribeTerminal(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	const id = "notify-term-1"
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != id {
			t.Fatalf("payload=%q want %q", got, id)
		}
	case <-time.After(time.Second):
		t.Fatal("expected terminal notify after TerminateInstance")
	}
}

func TestSubscribe_CancelStopsDelivery(t *testing.T) {
	b := memory.New()
	subCtx, cancel := context.WithCancel(context.Background())
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	// Allow unregister goroutine to run.
	time.Sleep(20 * time.Millisecond)
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: "notify-cancel-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("did not expect wake after cancel")
	case <-time.After(50 * time.Millisecond):
	}
}
