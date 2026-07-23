package workflow_test

import (
	"encoding/json"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestGetVersion_RecordsMax(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.GetVersion(ctx, "change", 1, 2), nil
	})
	if res.Result != 2 {
		t.Fatalf("got %v", res.Result)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeVersionMarker {
		t.Fatalf("%+v", res.NewCommands)
	}
}

func TestGetVersion_ReplayReturnsRecorded(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"change_id": "change", "version": 2})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeVersionMarker, Name: "change", Payload: payload},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.GetVersion(ctx, "change", 1, 3), nil
	})
	if res.Result != 2 {
		t.Fatalf("got %v", res.Result)
	}
}

func TestGetVersion_MarkerSkippedByOldCode(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"change_id": "change", "version": 2})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeVersionMarker, Name: "change", Payload: payload},
		{Seq: 3, Type: journal.TypeTimerCreated},
		{Seq: 4, Type: journal.TypeTimerFired, RefSeq: 3},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// old code: no GetVersion, just Sleep
		return nil, workflow.Sleep(ctx, 0)
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
}
