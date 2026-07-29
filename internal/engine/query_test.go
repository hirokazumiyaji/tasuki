package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRunQuery_Happy(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunQuery(events, time.Time{}, "n", nil, func(ctx *workflow.Context) (any, error) {
		workflow.SetQueryHandler(ctx, "n", func(_ struct{}) (int, error) { return 42, nil })
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if res.Err != nil || res.Stuck || string(res.Payload) != "42" {
		t.Fatalf("%+v", res)
	}
}

func TestRunQuery_Unknown(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunQuery(events, time.Time{}, "missing", nil, func(ctx *workflow.Context) (any, error) {
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if !errors.Is(res.Err, workflow.ErrUnknownQuery) {
		t.Fatalf("%v", res.Err)
	}
}
