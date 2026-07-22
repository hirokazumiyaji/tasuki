package journal_test

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMatchCommand_OK(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeActivityScheduled, Name: "Charge"}
	if err := journal.MatchCommand(recorded, cmd); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestMatchCommand_TypeMismatch(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeTimerCreated, Name: ""}
	err := journal.MatchCommand(recorded, cmd)
	if !errors.Is(err, journal.ErrDeterminismViolation) {
		t.Fatalf("want ErrDeterminismViolation, got %v", err)
	}
}

func TestMatchCommand_NameMismatch(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeActivityScheduled, Name: "Refund"}
	err := journal.MatchCommand(recorded, cmd)
	if !errors.Is(err, journal.ErrDeterminismViolation) {
		t.Fatalf("want ErrDeterminismViolation, got %v", err)
	}
}
