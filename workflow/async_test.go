package workflow_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestAwait_FirstCompleted(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "a"},
		{Seq: 3, Type: journal.TypeTimerCreated},
		{Seq: 4, Type: journal.TypeTimerFired, RefSeq: 3},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[struct{}, string](ctx, "a", struct{}{})
		s := workflow.SleepAsync(ctx, time.Second)
		idx, err := workflow.Await(ctx, f, s)
		return idx, err
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if res.Result != 1 {
		t.Fatalf("want timer index 1, got %v", res.Result)
	}
}

func TestAwait_SuspendsWhenNoneReady(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[struct{}, string](ctx, "a", struct{}{})
		s := workflow.SleepAsync(ctx, time.Hour)
		return workflow.Await(ctx, f, s)
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
	if len(res.NewCommands) != 2 {
		t.Fatalf("want 2 commands, got %+v", res.NewCommands)
	}
}

func TestAwaitAll(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "a"},
		{Seq: 3, Type: journal.TypeActivityScheduled, Name: "b"},
		{Seq: 4, Type: journal.TypeActivityCompleted, RefSeq: 2, Payload: []byte(`"x"`)},
		{Seq: 5, Type: journal.TypeActivityCompleted, RefSeq: 3, Payload: []byte(`"y"`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		fa := workflow.ExecuteAsync[struct{}, string](ctx, "a", struct{}{})
		fb := workflow.ExecuteAsync[struct{}, string](ctx, "b", struct{}{})
		if err := workflow.AwaitAll(ctx, fa, fb); err != nil {
			return nil, err
		}
		a, _ := fa.Get(ctx)
		b, _ := fb.Get(ctx)
		return a + b, nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if res.Result != "xy" {
		t.Fatalf("got %v", res.Result)
	}
}
