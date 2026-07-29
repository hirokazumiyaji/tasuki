package observability_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/observability"
)

func TestNewMetrics(t *testing.T) {
	m, err := observability.NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	if m.WorkflowTasks == nil || m.ActivityTasks == nil || m.JournalWarnings == nil || m.TaskBacklog == nil {
		t.Fatal("instruments nil")
	}
}

func TestMetrics_NilReceiverNoPanic(t *testing.T) {
	var m *observability.Metrics
	ctx := context.Background()
	m.AddWorkflowTask(ctx, 1)
	m.AddActivityTask(ctx, 1)
	m.AddTerminal(ctx, "completed")
	m.AddActivityRetry(ctx, 1)
	m.AddJournalWarning(ctx, 1)
	m.AddIncompatibleNack(ctx, "unknown_type")
	m.RecordBacklog(ctx, "workflow", "default", 3)
}

func TestMetrics_AddHelpers(t *testing.T) {
	m := observability.MustNewMetrics()
	ctx := context.Background()
	m.AddWorkflowTask(ctx, 1)
	m.AddActivityTask(ctx, 2)
	m.AddTerminal(ctx, "failed")
	m.AddActivityRetry(ctx, 1)
	m.AddJournalWarning(ctx, 1)
	m.AddIncompatibleNack(ctx, "determinism")
	m.RecordBacklog(ctx, "activity", "default", 9)
}
