package engine_test

import (
	"encoding/json"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestContinueUpdates_NilContext(t *testing.T) {
	res := engine.ContinueUpdates(nil)
	if res.Suspended || res.Stuck || res.Err != nil {
		t.Fatalf("%+v", res)
	}
}

func TestContinueUpdates_AfterRun(t *testing.T) {
	req, _ := json.Marshal(map[string]any{"id": "u1", "input": 3})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeUpdateRequested, Name: "inc", Payload: req},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		workflow.SetUpdateHandler(ctx, "inc", func(ctx *workflow.Context, n int) (int, error) {
			return n + 1, nil
		})
		return "ok", nil
	})
	if res.WorkflowContext() == nil {
		t.Fatal("nil ctx")
	}
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Stuck || ures.Suspended {
		t.Fatalf("%+v", ures)
	}
	cmds := res.WorkflowContext().NewCommands()
	if len(cmds) < 2 {
		t.Fatalf("%+v", cmds)
	}
}

func TestContinueUpdates_NoPending(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return "done", nil
	})
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Suspended || len(ures.NewCommands) != 0 {
		t.Fatalf("%+v", ures)
	}
}

func TestContinueUpdates_UnknownHandler(t *testing.T) {
	req, _ := json.Marshal(map[string]any{"id": "u1", "input": 1})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeUpdateRequested, Name: "missing", Payload: req},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return "ok", nil
	})
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Stuck || ures.Suspended {
		t.Fatalf("%+v", ures)
	}
	cmds := res.WorkflowContext().NewCommands()
	if len(cmds) < 2 {
		t.Fatalf("want accept+complete, got %+v", cmds)
	}
}
