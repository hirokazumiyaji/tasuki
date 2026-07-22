package engine_test

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_DeterminismViolationStuck(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "Charge"},
		{Seq: 3, Type: journal.TypeActivityCompleted, RefSeq: 2, Payload: []byte(`"ok"`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		_, err := workflow.Execute[struct{}, string](ctx, "Refund", struct{}{})
		return nil, err
	})
	if !res.Stuck {
		t.Fatal("expected stuck")
	}
	if !errors.Is(res.Err, journal.ErrDeterminismViolation) {
		t.Fatalf("want determinism violation, got %v", res.Err)
	}
}
