package backend

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestSortInbox(t *testing.T) {
	t.Parallel()
	entries := []InboxEntry{
		{Seq: 2, CreatedAt: 20, ID: 200, Event: journal.Event{Name: "b"}},
		{Seq: 0, CreatedAt: 30, ID: 300, Event: journal.Event{Name: "legacy-late"}},
		{Seq: 1, CreatedAt: 10, ID: 100, Event: journal.Event{Name: "a"}},
		{Seq: 0, CreatedAt: 10, ID: 50, Event: journal.Event{Name: "legacy-early"}},
	}
	SortInbox(entries)
	got := []string{
		entries[0].Event.Name,
		entries[1].Event.Name,
		entries[2].Event.Name,
		entries[3].Event.Name,
	}
	want := []string{"legacy-early", "legacy-late", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v want=%v", got, want)
		}
	}
}

func TestSortInboxStableTiebreak(t *testing.T) {
	t.Parallel()
	entries := []InboxEntry{
		{Seq: 1, CreatedAt: 10, ID: 2, Event: journal.Event{Name: "second"}},
		{Seq: 1, CreatedAt: 10, ID: 1, Event: journal.Event{Name: "first"}},
	}
	SortInbox(entries)
	if entries[0].Event.Name != "first" || entries[1].Event.Name != "second" {
		t.Fatalf("order=%v", []string{entries[0].Event.Name, entries[1].Event.Name})
	}
}
