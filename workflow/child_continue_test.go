package workflow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestContinueAsNew(t *testing.T) {
	ctx := workflow.NewContext(nil, time.Time{})
	err := workflow.ContinueAsNew(ctx, map[string]int{"n": 3})
	input, ok := workflow.AsContinueAsNew(err)
	if !ok {
		t.Fatalf("ok=false err=%v", err)
	}
	if string(input) != `{"n":3}` {
		t.Fatalf("input=%s", input)
	}
	if !errors.Is(err, workflow.ErrContinueAsNew) {
		t.Fatalf("unwrap: %v", err)
	}
	if _, ok := workflow.AsContinueAsNew(errors.New("other")); ok {
		t.Fatal("other should not match")
	}
}

func TestExecuteChild_SchedulesAndSuspends(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "parent"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		ctx.SetInfo(workflow.WorkflowInfo{InstanceID: "p1"})
		return workflow.ExecuteChild[int, int](ctx, "child", 7)
	})
	if !res.Suspended {
		t.Fatalf("want suspend: %+v", res)
	}
	cmds := res.NewCommands
	if len(cmds) != 1 || cmds[0].Type != journal.TypeChildScheduled || cmds[0].Name != "child" {
		t.Fatalf("%+v", cmds)
	}
}

func TestExecuteChildAsync_SchedulesCommand(t *testing.T) {
	ctx := workflow.NewContext([]journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "parent"},
	}, time.Time{})
	ctx.SetInfo(workflow.WorkflowInfo{InstanceID: "p1"})
	fut := workflow.ExecuteChildAsync[int, int](ctx, "child", 7)
	cmds := ctx.NewCommands()
	if len(cmds) != 1 || cmds[0].Type != journal.TypeChildScheduled {
		t.Fatalf("%+v", cmds)
	}
	if fut.Seq() != cmds[0].Seq {
		t.Fatalf("seq=%d want %d", fut.Seq(), cmds[0].Seq)
	}
}
