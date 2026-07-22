package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestSleep_UsesFireAtPayload(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.Sleep(ctx, 7*24*time.Hour)
	})
	if !res.Suspended || len(res.NewCommands) != 1 {
		t.Fatalf("%+v", res)
	}
	var p struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(res.NewCommands[0].Payload, &p); err != nil {
		t.Fatal(err)
	}
	want := now.Add(7 * 24 * time.Hour)
	if !p.FireAt.Equal(want) {
		t.Fatalf("fire_at=%v want=%v", p.FireAt, want)
	}
}
