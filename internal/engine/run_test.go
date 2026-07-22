package engine_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_SuspendsOnUnresolvedWait(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		if err := workflow.Sleep(ctx, 0); err != nil {
			return nil, err
		}
		return "done", nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeTimerCreated {
		t.Fatalf("want one timer_created, got %+v", res.NewCommands)
	}
}

func TestRun_CompletesWhenJournalHasResults(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated},
		{Seq: 3, Type: journal.TypeTimerFired, RefSeq: 2},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		if err := workflow.Sleep(ctx, 0); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	if res.Suspended {
		t.Fatal("should not suspend")
	}
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result != "ok" {
		t.Fatalf("got %v", res.Result)
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("no new commands on pure replay, got %+v", res.NewCommands)
	}
}

func TestRun_DeferRunsOnSuspend(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	var deferred bool
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		defer func() { deferred = true }()
		_ = workflow.Sleep(ctx, 0)
		return nil, nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
	if !deferred {
		t.Fatal("defer should run on Goexit suspend")
	}
}

func TestRun_RecoverDoesNotCatchSuspend(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("recover caught suspend sentinel: %v", r)
			}
		}()
		_ = workflow.Sleep(ctx, 0)
		return "should-not-reach", nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
}
