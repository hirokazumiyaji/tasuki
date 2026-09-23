package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestSleepUntil_UsesAbsoluteDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := now.Add(2 * time.Hour)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.SleepUntil(ctx, target)
	})
	if !res.Suspended {
		t.Fatalf("want suspend: %+v", res)
	}
	// Now recording + timer creation.
	if len(res.NewCommands) != 2 {
		t.Fatalf("want 2 commands, got %+v", res.NewCommands)
	}
	if res.NewCommands[0].Type != journal.TypeNowRecorded {
		t.Fatalf("first command=%v want now_recorded", res.NewCommands[0].Type)
	}
	if res.NewCommands[1].Type != journal.TypeTimerCreated {
		t.Fatalf("second command=%v want timer_created", res.NewCommands[1].Type)
	}
	var p struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(res.NewCommands[1].Payload, &p); err != nil {
		t.Fatal(err)
	}
	if !p.FireAt.Equal(target) {
		t.Fatalf("fire_at=%v want=%v", p.FireAt, target)
	}
}

func TestSleepUntil_ReplayCompletes(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := now.Add(2 * time.Hour)
	fireAt, _ := json.Marshal(struct {
		FireAt time.Time `json:"fire_at"`
	}{FireAt: target})
	recordedNow, _ := json.Marshal(now.UTC())
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeNowRecorded, Payload: recordedNow},
		{Seq: 3, Type: journal.TypeTimerCreated, Payload: fireAt},
		{Seq: 4, Type: journal.TypeTimerFired, RefSeq: 3},
	}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.SleepUntil(ctx, target)
	})
	if res.Suspended || res.Stuck || res.Err != nil {
		t.Fatalf("want completion: %+v err=%v", res, res.Err)
	}
}

func TestSleepUntil_PastDeadlineFiresImmediately(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	target := now.Add(-time.Minute)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.SleepUntil(ctx, target)
	})
	if !res.Suspended {
		t.Fatalf("want suspend: %+v", res)
	}
	if len(res.NewCommands) != 2 || res.NewCommands[1].Type != journal.TypeTimerCreated {
		t.Fatalf("%+v", res.NewCommands)
	}
	var p struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(res.NewCommands[1].Payload, &p); err != nil {
		t.Fatal(err)
	}
	// Already-due deadlines clamp to the workflow clock; the timer stays
	// already-due and fires on the next tick.
	if !p.FireAt.Equal(now) {
		t.Fatalf("fire_at=%v want=%v", p.FireAt, now)
	}
}

func TestSleepUntil_ZeroDeadlineClampsToNow(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.SleepUntil(ctx, time.Time{})
	})
	if !res.Suspended {
		t.Fatalf("want suspend: %+v", res)
	}
	if len(res.NewCommands) != 2 || res.NewCommands[1].Type != journal.TypeTimerCreated {
		t.Fatalf("%+v", res.NewCommands)
	}
	var p struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(res.NewCommands[1].Payload, &p); err != nil {
		t.Fatal(err)
	}
	// time.Time{} (year 1) would break MySQL DATETIME(6) inserts (minimum
	// year 1000); it must clamp to now and stay already-due.
	if !p.FireAt.Equal(now) {
		t.Fatalf("fire_at=%v want=%v", p.FireAt, now)
	}
}
