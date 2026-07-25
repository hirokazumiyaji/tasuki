package spanner_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
)

func openNotifyBackend(t *testing.T) *spanner.Backend {
	t.Helper()
	dsn := dsnOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := spanner.New(ctx, dsn)
	if err != nil {
		t.Skip(err.Error())
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Skip(err.Error())
	}
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
	}
	return b
}

func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "notify-wake-1", Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected notify after CreateInstance")
	}
}

func TestSubscribeTerminal_WakesOnTerminate(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.SubscribeTerminal(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	const id = "notify-term-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "wf", Queue: "default", Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != id {
			t.Fatalf("payload=%q want %q", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected terminal notify after TerminateInstance")
	}
}

func TestSubscribe_CancelStopsDelivery(t *testing.T) {
	ctx := context.Background()
	b := openNotifyBackend(t)
	subCtx, cancel := context.WithCancel(ctx)
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if err := b.CreateInstance(ctx, backend.NewInstance{
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
