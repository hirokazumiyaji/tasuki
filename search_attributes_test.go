package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestSearchAttributes_StartUpsertList(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		workflow.UpsertSearchAttributes(wctx, map[string]string{
			"phase":    "shipped",
			"order_id": "",
		})
		return "ok", nil
	}, tasuki.WithName("saWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "saWF", struct{}{},
		tasuki.WithID("sa-int-1"),
		tasuki.WithSearchAttributes(map[string]string{
			"tenant":   "acme",
			"order_id": "42",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasuki.Result[string](ctx, h); err != nil {
		t.Fatal(err)
	}

	inst, err := c.Get(ctx, "sa-int-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["tenant"] != "acme" || inst.SearchAttributes["phase"] != "shipped" {
		t.Fatalf("got %#v", inst.SearchAttributes)
	}
	if _, ok := inst.SearchAttributes["order_id"]; ok {
		t.Fatal("order_id should be deleted")
	}

	list, err := c.List(ctx, tasuki.InstanceFilter{
		Status:           tasuki.StatusCompleted,
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "shipped"},
	})
	if err != nil || len(list) != 1 || list[0].ID != "sa-int-1" {
		t.Fatalf("list: %+v err=%v", list, err)
	}
}
