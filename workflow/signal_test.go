package workflow_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestReceiveSignal(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeSignalReceived, Name: "approve", Payload: []byte(`"yes"`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.ReceiveSignal[string](ctx, "approve")
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if res.Result != "yes" {
		t.Fatalf("got %v", res.Result)
	}
}

func TestReceiveSignal_Suspends(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.ReceiveSignal[string](ctx, "approve")
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
}
