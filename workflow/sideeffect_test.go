package workflow_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestNow_IsDurable(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return workflow.Now(ctx), nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if !res.Result.(time.Time).Equal(now) {
		t.Fatalf("got %v", res.Result)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeNowRecorded {
		t.Fatalf("%+v", res.NewCommands)
	}
}

func TestSideEffect_ReplaysStoredValue(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeSideEffect, Payload: []byte(`"fixed"`)},
	}
	calls := 0
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.SideEffect(ctx, func() string {
			calls++
			return "new"
		})
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result != "fixed" {
		t.Fatalf("got %v", res.Result)
	}
	if calls != 0 {
		t.Fatalf("fn should not run on replay, calls=%d", calls)
	}
}

func TestNewUUID_StableOnReplay(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.NewUUID(ctx)
	})
	if res.Suspended || res.Err != nil || len(res.NewCommands) != 1 {
		t.Fatalf("%+v", res)
	}
	id := res.Result.(string)
	events = append(events, res.NewCommands...)
	res2 := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.NewUUID(ctx)
	})
	if res2.Result != id {
		t.Fatalf("got %v want %v", res2.Result, id)
	}
}
