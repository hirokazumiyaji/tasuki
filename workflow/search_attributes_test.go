package workflow_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestUpsertSearchAttributes_MergeAndReplay(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetSearchAttributes(map[string]string{"tenant": "acme", "order_id": "1"})
		workflow.UpsertSearchAttributes(ctx, map[string]string{
			"phase":    "shipped",
			"order_id": "",
		})
		return ctx.SearchAttributes(), nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	got := res.Result.(map[string]string)
	if got["tenant"] != "acme" || got["phase"] != "shipped" || len(got) != 2 {
		t.Fatalf("got %#v", got)
	}
	if _, ok := got["order_id"]; ok {
		t.Fatal("order_id should be deleted")
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeSearchAttributesUpdated {
		t.Fatalf("commands: %+v", res.NewCommands)
	}

	events = append(events, res.NewCommands...)
	res2 := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// Start attrs not re-applied; journal payload is authoritative after upsert.
		workflow.UpsertSearchAttributes(ctx, map[string]string{
			"phase":    "shipped",
			"order_id": "",
		})
		return ctx.SearchAttributes(), nil
	})
	if res2.Err != nil || len(res2.NewCommands) != 0 {
		t.Fatalf("%+v", res2)
	}
	got2 := res2.Result.(map[string]string)
	if got2["tenant"] != "acme" || got2["phase"] != "shipped" {
		t.Fatalf("replay got %#v", got2)
	}
}
