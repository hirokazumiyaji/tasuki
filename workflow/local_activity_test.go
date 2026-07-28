package workflow_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestExecuteLocal_RecordsAndReturnsResult(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	calls := 0
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetLocalActivityRunner(func(name string, input []byte) ([]byte, error) {
			calls++
			if name != "double" {
				t.Fatalf("name=%q", name)
			}
			var n int
			if err := json.Unmarshal(input, &n); err != nil {
				return nil, err
			}
			return json.Marshal(n * 2)
		})
		return workflow.ExecuteLocal[int, int](ctx, "double", 21)
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	if res.Result != 42 {
		t.Fatalf("got %v", res.Result)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeLocalActivity {
		t.Fatalf("%+v", res.NewCommands)
	}
	if res.NewCommands[0].Name != "double" {
		t.Fatalf("name=%q", res.NewCommands[0].Name)
	}
}

func TestExecuteLocal_ReplaySkipsRunner(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"input":  7,
		"result": 14,
	})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeLocalActivity, Name: "double", Payload: payload},
	}
	calls := 0
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetLocalActivityRunner(func(string, []byte) ([]byte, error) {
			calls++
			return nil, errors.New("should not run")
		})
		return workflow.ExecuteLocal[int, int](ctx, "double", 7)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result != 14 {
		t.Fatalf("got %v", res.Result)
	}
	if calls != 0 {
		t.Fatalf("runner should not run on replay, calls=%d", calls)
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("unexpected commands %+v", res.NewCommands)
	}
}

func TestExecuteLocal_ErrorPath(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetLocalActivityRunner(func(string, []byte) ([]byte, error) {
			return nil, errors.New("lookup failed")
		})
		return workflow.ExecuteLocal[string, string](ctx, "lookup", "x")
	})
	if res.Err == nil || res.Err.Error() != "lookup failed" {
		t.Fatalf("err=%v", res.Err)
	}
	if len(res.NewCommands) != 1 {
		t.Fatalf("want journaled failure, got %+v", res.NewCommands)
	}
	var p struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(res.NewCommands[0].Payload, &p)
	if p.Error != "lookup failed" {
		t.Fatalf("payload error=%q", p.Error)
	}
}

func TestExecuteLocal_MissingRunner(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.ExecuteLocal[int, int](ctx, "double", 1)
	})
	if !errors.Is(res.Err, workflow.ErrLocalActivityRunnerMissing) {
		t.Fatalf("err=%v", res.Err)
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("should not journal without runner: %+v", res.NewCommands)
	}
}

func TestExecuteLocal_ReplayError(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"input": "x",
		"error": "not found",
	})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeLocalActivity, Name: "lookup", Payload: payload},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.ExecuteLocal[string, string](ctx, "lookup", "x")
	})
	if res.Err == nil || res.Err.Error() != "not found" {
		t.Fatalf("err=%v", res.Err)
	}
}
