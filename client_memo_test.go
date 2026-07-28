package tasuki_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestStart_WithMemo(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{},
		tasuki.WithID("start-memo"),
		tasuki.WithMemo(map[string]string{"note": "vip"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip" {
		t.Fatalf("got %#v", inst.Memo)
	}
}
