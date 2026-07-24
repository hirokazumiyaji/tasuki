package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
)

func TestSubscribe_WakesOnCreateInstance(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.Subscribe(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	// Allow LISTEN to be established before notify.
	time.Sleep(100 * time.Millisecond)

	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID:    "notify-wake-1",
		Name:  "wf",
		Queue: "default",
		Input: []byte(`0`),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected notify after CreateInstance")
	}
}
