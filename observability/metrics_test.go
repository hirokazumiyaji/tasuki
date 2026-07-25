package observability_test

import (
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
