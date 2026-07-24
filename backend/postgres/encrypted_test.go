package postgres_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/codec"
)

func TestEncryptedPayloadRoundTripsThroughJSONB(t *testing.T) {
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

	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": bytes.Repeat([]byte{'a'}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	enc := codec.Encrypted(codec.JSON(), kr)
	payload, err := enc.Marshal(map[string]string{"card": "4242"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "enc-pg-1", Name: "wf", Queue: "default", Input: payload}); err != nil {
		t.Fatal(err)
	}
	events, err := b.GetJournal(ctx, "enc-pg-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("no journal events")
	}
	var out map[string]string
	if err := enc.Unmarshal(events[0].Payload, &out); err != nil {
		t.Fatalf("decrypt after jsonb roundtrip: %v", err)
	}
	if out["card"] != "4242" {
		t.Fatalf("got %v", out)
	}
}
