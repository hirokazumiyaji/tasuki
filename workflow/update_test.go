package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestUpdate_AcceptAndComplete(t *testing.T) {
	req, _ := json.Marshal(map[string]any{"id": "u1", "input": 7})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeUpdateRequested, Name: "double", Payload: req},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		workflow.SetUpdateHandler(ctx, "double", func(ctx *workflow.Context, n int) (int, error) {
			return n * 2, nil
		})
		return "main", nil
	})
	if res.Suspended || res.Err != nil {
		t.Fatalf("%+v", res)
	}
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Suspended || ures.Stuck {
		t.Fatalf("%+v", ures)
	}
	cmds := res.WorkflowContext().NewCommands()
	if len(cmds) != 2 {
		t.Fatalf("commands=%+v", cmds)
	}
	if cmds[0].Type != journal.TypeUpdateAccepted || cmds[1].Type != journal.TypeUpdateCompleted {
		t.Fatalf("commands=%+v", cmds)
	}
	result, errMsg, ok := workflow.FindUpdateCompletion(append(events, cmds...), "u1")
	if !ok || errMsg != "" {
		t.Fatalf("ok=%v err=%q", ok, errMsg)
	}
	var n int
	_ = json.Unmarshal(result, &n)
	if n != 14 {
		t.Fatalf("got %d", n)
	}
}

func TestUpdate_HandlerExecuteSuspendResume(t *testing.T) {
	req, _ := json.Marshal(map[string]any{"id": "u2", "input": 3})
	fireAt := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	timerPayload, _ := json.Marshal(map[string]any{"fire_at": fireAt})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	runMain := func(evs []journal.Event) engine.Result {
		return engine.RunAt(evs, now, func(ctx *workflow.Context) (any, error) {
			workflow.SetUpdateHandler(ctx, "work", func(ctx *workflow.Context, n int) (int, error) {
				return workflow.Execute[int, int](ctx, "inc", n)
			})
			return nil, workflow.Sleep(ctx, 24*time.Hour)
		})
	}

	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated, Payload: timerPayload},
		{Seq: 3, Type: journal.TypeUpdateRequested, Name: "work", Payload: req},
	}
	res := runMain(events)
	if !res.Suspended {
		t.Fatalf("main should wait on timer: %+v", res)
	}
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if !ures.Suspended {
		t.Fatalf("update should wait on activity: %+v", ures)
	}
	cmds := res.WorkflowContext().NewCommands()
	var actSeq int64
	for _, c := range cmds {
		if c.Type == journal.TypeActivityScheduled && c.Name == "inc" {
			actSeq = c.Seq
		}
	}
	if actSeq == 0 {
		t.Fatalf("missing activity schedule: %+v", cmds)
	}

	events2 := append([]journal.Event{}, events...)
	events2 = append(events2, cmds...)
	events2 = append(events2, journal.Event{
		Seq: cmds[len(cmds)-1].Seq + 1, Type: journal.TypeActivityCompleted, RefSeq: actSeq, Payload: []byte(`4`),
	})

	res2 := runMain(events2)
	if !res2.Suspended {
		t.Fatalf("main still waiting timer: %+v", res2)
	}
	ures2 := engine.ContinueUpdates(res2.WorkflowContext())
	if ures2.Suspended || ures2.Stuck {
		t.Fatalf("%+v", ures2)
	}
	result, errMsg, ok := workflow.FindUpdateCompletion(append(events2, res2.WorkflowContext().NewCommands()...), "u2")
	if !ok || errMsg != "" {
		t.Fatalf("ok=%v err=%q", ok, errMsg)
	}
	var n int
	_ = json.Unmarshal(result, &n)
	if n != 4 {
		t.Fatalf("got %d", n)
	}
}
