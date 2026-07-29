package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWithStartToCloseTimeout_OnSchedulePayload(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.Execute[int, int](ctx, "slow", 1,
			workflow.WithStartToCloseTimeout(1500*time.Millisecond),
			workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 2}),
		)
	})
	if !res.Suspended {
		t.Fatalf("want suspend waiting for activity, got %+v", res)
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeActivityScheduled {
		t.Fatalf("commands: %+v", res.NewCommands)
	}
	var sched workflow.ActivitySchedule
	if err := json.Unmarshal(res.NewCommands[0].Payload, &sched); err != nil {
		t.Fatal(err)
	}
	if sched.StartToCloseTimeoutMs != 1500 {
		t.Fatalf("timeout_ms=%d", sched.StartToCloseTimeoutMs)
	}
	if sched.Retry == nil || sched.Retry.MaxAttempts != 2 {
		t.Fatalf("retry=%+v", sched.Retry)
	}
}

func TestWithStartToCloseTimeout_ZeroOmitsField(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return workflow.Execute[int, int](ctx, "slow", 1, workflow.WithStartToCloseTimeout(0))
	})
	if !res.Suspended || len(res.NewCommands) != 1 {
		t.Fatalf("%+v", res)
	}
	var sched workflow.ActivitySchedule
	_ = json.Unmarshal(res.NewCommands[0].Payload, &sched)
	if sched.StartToCloseTimeoutMs != 0 {
		t.Fatalf("want omitted/zero, got %d", sched.StartToCloseTimeoutMs)
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(res.NewCommands[0].Payload, &raw)
	if _, ok := raw["start_to_close_timeout_ms"]; ok {
		t.Fatal("zero timeout should be omitted from JSON")
	}
}

func TestExecuteAsync_WithStartToCloseTimeout(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		f := workflow.ExecuteAsync[int, int](ctx, "slow", 1, workflow.WithStartToCloseTimeout(2*time.Second))
		return f.Seq(), nil
	})
	if res.Err != nil || len(res.NewCommands) != 1 {
		t.Fatalf("%+v", res)
	}
	var sched workflow.ActivitySchedule
	_ = json.Unmarshal(res.NewCommands[0].Payload, &sched)
	if sched.StartToCloseTimeoutMs != 2000 {
		t.Fatalf("timeout_ms=%d", sched.StartToCloseTimeoutMs)
	}
}
