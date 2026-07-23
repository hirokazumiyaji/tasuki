package journal_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestEventTypeConstants(t *testing.T) {
	cases := []journal.Type{
		journal.TypeWorkflowStarted,
		journal.TypeActivityScheduled,
		journal.TypeTimerCreated,
		journal.TypeActivityCompleted,
		journal.TypeTimerFired,
		journal.TypeWorkflowCompleted,
		journal.TypeWorkflowFailed,
	}
	for _, typ := range cases {
		if typ == "" {
			t.Fatalf("empty event type")
		}
	}
}

func TestIsCommand(t *testing.T) {
	if !journal.TypeActivityScheduled.IsCommand() {
		t.Fatal("activity_scheduled should be command")
	}
	if journal.TypeActivityCompleted.IsCommand() {
		t.Fatal("activity_completed should not be command")
	}
}

func TestChildEventClassification(t *testing.T) {
	if !journal.TypeChildScheduled.IsCommand() {
		t.Fatal("child_scheduled should be command")
	}
	if !journal.TypeChildCompleted.IsCompletion() {
		t.Fatal("child_completed should be completion")
	}
	if !journal.TypeChildFailed.IsCompletion() {
		t.Fatal("child_failed should be completion")
	}
}
