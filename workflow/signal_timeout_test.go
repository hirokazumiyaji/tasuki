package workflow_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestReceiveSignalWithTimeout_Suspend(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		_, ok, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		return ok, err
	})
	if !res.Suspended {
		t.Fatalf("%+v", res)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeTimerCreated {
		t.Fatalf("%+v", res.NewCommands)
	}
}

func TestReceiveSignalWithTimeout_Timeout(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fireAt := now.Add(time.Hour)
	payload, _ := json.Marshal(map[string]any{"fire_at": fireAt})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated, Payload: payload},
		{Seq: 3, Type: journal.TypeTimerFired, RefSeq: 2},
	}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		_, ok, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		if err != nil {
			return nil, err
		}
		return ok, nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if res.Result != false {
		t.Fatalf("want timed out (false), got %v", res.Result)
	}
}

func TestReceiveSignalWithTimeout_Canceled(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeCancelRequested},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		_, _, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		return nil, err
	})
	if !errors.Is(res.Err, workflow.ErrCanceled) {
		t.Fatalf("%+v", res)
	}
}

func TestReceiveSignalWithTimeout_SignalAfterTimerThenActivity(t *testing.T) {
	// Regression for #280: timer recorded while waiting, then a signal arrives.
	// Replay must consume the recorded timer before taking the signal, otherwise
	// the next command mismatches with ErrDeterminismViolation (permanent Nack).
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fireAt := now.Add(time.Hour)
	tp, _ := json.Marshal(map[string]any{"fire_at": fireAt})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated, Payload: tp},
		{Seq: 3, Type: journal.TypeSignalReceived, Name: "go", Payload: []byte(`"hi"`)},
	}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		v, ok, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		if err != nil || !ok || v != "hi" {
			return nil, errors.New("want signal hi")
		}
		return workflow.Execute[string, string](ctx, "act", "x")
	})
	if res.Stuck {
		t.Fatalf("determinism violation: %+v", res.Err)
	}
	if !res.Suspended {
		t.Fatalf("want suspend waiting for activity, got %+v", res)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeActivityScheduled {
		t.Fatalf("%+v", res.NewCommands)
	}
}

func TestReceiveSignalWithTimeout_ImmediateSignalThenSleep(t *testing.T) {
	// Regression for Codex P1 on #310: when the signal was already present on
	// the first execution, this wait records no timer, so the next recorded
	// timer_created belongs to the following Sleep. Replay must not consume
	// that later timer as this wait's timer; otherwise the following Sleep
	// emits a fresh command and suspends instead of completing.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tp, _ := json.Marshal(map[string]any{"fire_at": now.Add(time.Hour)})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeSignalReceived, Name: "go", Payload: []byte(`"hi"`)},
		{Seq: 3, Type: journal.TypeTimerCreated, Payload: tp},
		{Seq: 4, Type: journal.TypeTimerFired, RefSeq: 3},
	}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		v, ok, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		if err != nil || !ok || v != "hi" {
			return nil, errors.New("want signal hi")
		}
		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return nil, err
		}
		return v, nil
	})
	if res.Stuck || res.Suspended || res.Err != nil {
		t.Fatalf("want complete, got %+v (err=%v)", res, res.Err)
	}
	if res.Result != "hi" {
		t.Fatalf("got %v", res.Result)
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("want no new commands, got %+v", res.NewCommands)
	}
}

func TestReceiveSignalWithTimeout_SignalAfterTimerScheduled(t *testing.T) {
	// Replay: no signal at peek → schedule timer in history → signal present before timer fires.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fireAt := now.Add(time.Hour)
	tp, _ := json.Marshal(map[string]any{"fire_at": fireAt})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated, Payload: tp},
		{Seq: 3, Type: journal.TypeSignalReceived, Name: "go", Payload: []byte(`"hi"`)},
	}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		v, ok, err := workflow.ReceiveSignalWithTimeout[string](ctx, "go", time.Hour)
		if err != nil || !ok {
			return nil, errors.New("want signal")
		}
		return v, nil
	})
	if res.Suspended || res.Err != nil || res.Result != "hi" {
		t.Fatalf("%+v", res)
	}
}
