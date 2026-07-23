package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

func TestScheduleInstanceID(t *testing.T) {
	at := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	got := backend.ScheduleInstanceID("hourly", at)
	want := "hourly:2026-01-01T01:00:00Z"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNextCronTime(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := backend.NextCronTime("0 * * * *", from)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("got %v want %v", next, want)
	}
}
