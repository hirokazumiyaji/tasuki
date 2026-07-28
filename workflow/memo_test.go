package workflow_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestUpsertMemo_MergeAndReplay(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetMemo(map[string]string{"note": "vip", "debug": "1"})
		workflow.UpsertMemo(ctx, map[string]string{
			"note":  "vip-shipped",
			"debug": "",
		})
		return ctx.Memo(), nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	got := res.Result.(map[string]string)
	if got["note"] != "vip-shipped" || len(got) != 1 {
		t.Fatalf("got %#v", got)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeMemoUpdated {
		t.Fatalf("commands: %+v", res.NewCommands)
	}

	events = append(events, res.NewCommands...)
	res2 := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		workflow.UpsertMemo(ctx, map[string]string{
			"note":  "vip-shipped",
			"debug": "",
		})
		return ctx.Memo(), nil
	})
	if res2.Err != nil || len(res2.NewCommands) != 0 {
		t.Fatalf("%+v", res2)
	}
	got2 := res2.Result.(map[string]string)
	if got2["note"] != "vip-shipped" {
		t.Fatalf("replay got %#v", got2)
	}
}
