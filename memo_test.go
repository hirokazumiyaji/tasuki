package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestMemo_StartUpsertGet(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.UpsertMemo(wctx, map[string]string{
			"note":  "vip-shipped",
			"debug": "",
		})
		return "ok", nil
	}, tasuki.WithName("memoWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "memoWF", struct{}{},
		tasuki.WithID("memo-int-1"),
		tasuki.WithMemo(map[string]string{
			"note":  "vip",
			"debug": "1",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasuki.Result[string](ctx, h); err != nil {
		t.Fatal(err)
	}

	inst, err := c.Get(ctx, "memo-int-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip-shipped" {
		t.Fatalf("got %#v", inst.Memo)
	}
	if _, ok := inst.Memo["debug"]; ok {
		t.Fatal("debug should be deleted")
	}
}
