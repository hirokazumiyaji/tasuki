package workflow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestQuery_ReturnsStateAtSuspend(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}
	res := engine.RunQuery(events, time.Time{}, "n", []byte(`{}`), func(ctx *workflow.Context) (any, error) {
		n := 0
		workflow.SetQueryHandler(ctx, "n", func(_ struct{}) (int, error) { return n, nil })
		n = 1
		_ = workflow.Sleep(ctx, 0)
		n = 2
		return n, nil
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if string(res.Payload) != "1" {
		t.Fatalf("payload=%s want 1", res.Payload)
	}
}

func TestQuery_ReturnsStateAfterTimer(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated},
		{Seq: 3, Type: journal.TypeTimerFired, RefSeq: 2},
	}
	res := engine.RunQuery(events, time.Time{}, "n", nil, func(ctx *workflow.Context) (any, error) {
		n := 0
		workflow.SetQueryHandler(ctx, "n", func(_ struct{}) (int, error) { return n, nil })
		n = 1
		_ = workflow.Sleep(ctx, 0)
		n = 2
		return n, nil
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if string(res.Payload) != "2" {
		t.Fatalf("payload=%s want 2", res.Payload)
	}
}

func TestQuery_UnknownName(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}
	res := engine.RunQuery(events, time.Time{}, "missing", nil, func(ctx *workflow.Context) (any, error) {
		workflow.SetQueryHandler(ctx, "n", func(_ struct{}) (int, error) { return 0, nil })
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if !errors.Is(res.Err, workflow.ErrUnknownQuery) {
		t.Fatalf("got %v", res.Err)
	}
}

func TestQuery_HandlerMustNotRecordCommands(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}
	res := engine.RunQuery(events, time.Time{}, "bad", nil, func(ctx *workflow.Context) (any, error) {
		workflow.SetQueryHandler(ctx, "bad", func(_ struct{}) (int, error) {
			_ = workflow.Sleep(ctx, time.Second)
			return 0, nil
		})
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if res.Err == nil {
		t.Fatal("expected error")
	}
}
