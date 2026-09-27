package client_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
)

func TestStart_WithSearchAttributes(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	c := client.NewClient(b)
	h, err := client.Start(ctx, c, "WF", struct{}{},
		client.WithID("start-sa"),
		client.WithSearchAttributes(map[string]string{"tenant": "acme"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["tenant"] != "acme" {
		t.Fatalf("got %#v", inst.SearchAttributes)
	}
}
