package backend_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMergeSearchAttributes(t *testing.T) {
	got := backend.MergeSearchAttributes(
		map[string]string{"a": "1", "b": "2"},
		map[string]string{"b": "9", "c": "3", "a": ""},
	)
	if got["b"] != "9" || got["c"] != "3" || len(got) != 2 {
		t.Fatalf("got %#v", got)
	}
	if _, ok := got["a"]; ok {
		t.Fatal("a should be deleted")
	}
}

func TestMatchesSearchAttributes(t *testing.T) {
	attrs := map[string]string{"tenant": "acme", "phase": "shipped"}
	if !backend.MatchesSearchAttributes(attrs, map[string]string{"tenant": "acme"}) {
		t.Fatal("expected match")
	}
	if backend.MatchesSearchAttributes(attrs, map[string]string{"tenant": "acme", "phase": "pending"}) {
		t.Fatal("expected no match")
	}
	if backend.MatchesSearchAttributes(attrs, map[string]string{"missing": "x"}) {
		t.Fatal("missing key should not match")
	}
}

func TestLastSearchAttributesUpdate(t *testing.T) {
	events := []journal.Event{
		{Type: journal.TypeSearchAttributesUpdated, Payload: []byte(`{"a":"1"}`)},
		{Type: journal.TypeActivityScheduled},
		{Type: journal.TypeSearchAttributesUpdated, Payload: []byte(`{"a":"2","b":"3"}`)},
	}
	got := backend.LastSearchAttributesUpdate(events)
	if got["a"] != "2" || got["b"] != "3" {
		t.Fatalf("got %#v", got)
	}
}
