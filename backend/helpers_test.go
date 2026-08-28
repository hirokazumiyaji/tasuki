package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestInboxBatchLimit(t *testing.T) {
	if got := backend.InboxBatchLimit(backend.Capabilities{}); got != backend.DefaultInboxBatchLimit {
		t.Fatalf("default=%d", got)
	}
	if got := backend.InboxBatchLimit(backend.Capabilities{MaxAdvancementEffects: 40}); got != 10 {
		t.Fatalf("got %d want 10", got)
	}
	if got := backend.InboxBatchLimit(backend.Capabilities{MaxAdvancementEffects: 3}); got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}

func TestMarshalSearchAttributes(t *testing.T) {
	if string(backend.MarshalSearchAttributes(nil)) != "{}" {
		t.Fatal("nil")
	}
	b := backend.MarshalSearchAttributes(map[string]string{"a": "1"})
	if string(b) != `{"a":"1"}` {
		t.Fatalf("%s", b)
	}
}

func TestHasSearchAttributesUpdate(t *testing.T) {
	if backend.HasSearchAttributesUpdate(nil) {
		t.Fatal("empty")
	}
	evs := []journal.Event{{Type: journal.TypeSearchAttributesUpdated, Payload: []byte(`{}`)}}
	if !backend.HasSearchAttributesUpdate(evs) {
		t.Fatal("want true")
	}
}

func TestHasMemoUpdateAndLast(t *testing.T) {
	if backend.HasMemoUpdate(nil) || backend.LastMemoUpdate(nil) != nil {
		t.Fatal("empty")
	}
	evs := []journal.Event{
		{Type: journal.TypeMemoUpdated, Payload: []byte(`{"n":"1"}`)},
		{Type: journal.TypeActivityScheduled},
		{Type: journal.TypeMemoUpdated, Payload: []byte(`{"n":"2"}`)},
	}
	if !backend.HasMemoUpdate(evs) {
		t.Fatal("want has")
	}
	got := backend.LastMemoUpdate(evs)
	if got["n"] != "2" {
		t.Fatalf("%#v", got)
	}
	bad := []journal.Event{{Type: journal.TypeMemoUpdated, Payload: []byte(`not-json`)}}
	if backend.LastMemoUpdate(bad) != nil {
		t.Fatal("bad payload")
	}
}

func TestNormalizePurgeStatuses(t *testing.T) {
	got, err := backend.NormalizePurgeStatuses(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"completed", "failed", "terminated", "canceled"}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if _, err := backend.NormalizePurgeStatuses([]string{"running"}); err == nil {
		t.Fatal("running must be rejected")
	}
	sts, limit, err := backend.ValidatePurgeArgs(-time.Second, nil, 0)
	if err == nil {
		t.Fatal("negative olderThan must be rejected")
	}
	_ = sts
	_ = limit
	sts, limit, err = backend.ValidatePurgeArgs(time.Hour, []string{"completed", "completed"}, 0)
	if err != nil || limit != backend.DefaultPurgeLimit || len(sts) != 1 {
		t.Fatalf("%v %d %v", sts, limit, err)
	}
}
