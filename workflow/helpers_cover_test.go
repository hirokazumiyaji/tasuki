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

func TestRetryPolicy_Backoff(t *testing.T) {
	p := workflow.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaxInterval:        10 * time.Second,
	}
	if got := p.Backoff(1); got != time.Second {
		t.Fatalf("attempt1=%v", got)
	}
	if got := p.Backoff(2); got != 2*time.Second {
		t.Fatalf("attempt2=%v", got)
	}
	if got := p.Backoff(0); got != time.Second {
		t.Fatalf("attempt0=%v", got)
	}
	if got := p.Backoff(20); got != 10*time.Second {
		t.Fatalf("capped=%v", got)
	}
}

func TestInfoAndSetCodec(t *testing.T) {
	ctx := workflow.NewContext(nil, time.Time{})
	info := workflow.WorkflowInfo{InstanceID: "i1", Name: "WF"}
	ctx.SetInfo(info)
	if got := workflow.Info(ctx); got != info {
		t.Fatalf("%+v", got)
	}
	ctx.SetCodec(coverCodec{})
}

type coverCodec struct{}

func (coverCodec) Marshal(v any) ([]byte, error) { return nil, nil }
func (coverCodec) Unmarshal(data []byte, v any) error {
	return nil
}

func TestUpdateHelpers(t *testing.T) {
	acc, _ := json.Marshal(map[string]string{"id": "u1"})
	comp, _ := json.Marshal(map[string]string{"id": "u2"})
	events := []journal.Event{
		{Type: journal.TypeUpdateAccepted, Payload: acc},
		{Type: journal.TypeUpdateCompleted, Payload: comp},
	}
	if !workflow.UpdateAcceptedInFlight(events, "u1") {
		t.Fatal("u1 should be in flight")
	}
	if workflow.UpdateAcceptedInFlight(events, "u2") {
		t.Fatal("u2 completed")
	}
	err := workflow.FormatUpdateError("boom")
	if err.Error() != "boom" {
		t.Fatal(err)
	}
	if workflow.FormatUpdateError("").Error() != "update failed" {
		t.Fatal("empty")
	}
}

func TestReceiveSignalWithTimeout_AlreadyPresent(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeSignalReceived, Name: "go", Payload: []byte(`true`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		v, ok, err := workflow.ReceiveSignalWithTimeout[bool](ctx, "go", time.Hour)
		if err != nil || !ok || !v {
			return nil, errors.New("bad receive")
		}
		return "ok", nil
	})
	if res.Suspended || res.Err != nil || res.Result != "ok" {
		t.Fatalf("%+v", res)
	}
}

func TestAwaitAll_ActivityFailed(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "x"},
		{Seq: 3, Type: journal.TypeActivityFailed, RefSeq: 2, Payload: []byte(`"boom"`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[int, int](ctx, "x", 1)
		return nil, workflow.AwaitAll(ctx, f)
	})
	if res.Suspended || res.Err == nil || res.Err.Error() != "boom" {
		t.Fatalf("%+v", res)
	}
}

func TestAwaitAll_Canceled(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeCancelRequested},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[int, int](ctx, "x", 1)
		return nil, workflow.AwaitAll(ctx, f)
	})
	if res.Err != workflow.ErrCanceled {
		t.Fatalf("%+v", res)
	}
}

func TestAwaitAll_EmptyFailMessage(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "x"},
		{Seq: 3, Type: journal.TypeActivityFailed, RefSeq: 2, Payload: []byte(`""`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[int, int](ctx, "x", 1)
		return nil, workflow.AwaitAll(ctx, f)
	})
	if res.Suspended || res.Err == nil || res.Err.Error() != "operation failed" {
		t.Fatalf("%+v", res)
	}
}

func TestFutureGet_Canceled(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeCancelRequested},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[int, int](ctx, "x", 1)
		return f.Get(ctx)
	})
	if res.Err != workflow.ErrCanceled {
		t.Fatalf("%+v", res)
	}
}

func TestContinueAsNew_ErrorMethods(t *testing.T) {
	ctx := workflow.NewContext(nil, time.Time{})
	err := workflow.ContinueAsNew(ctx, 1)
	if err.Error() != workflow.ErrContinueAsNew.Error() {
		t.Fatal(err.Error())
	}
}
