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

func TestFairPick(t *testing.T) {
	mk := func(spec string) []backend.FairTaskRef {
		var out []backend.FairTaskRef
		for i, c := range []byte(spec) {
			out = append(out, backend.FairTaskRef{ID: int64(i + 1), InstanceID: string(c)})
		}
		return out
	}
	ids := func(refs []backend.FairTaskRef) string {
		b := make([]byte, 0, len(refs))
		for _, r := range refs {
			b = append(b, r.InstanceID[0])
		}
		return string(b)
	}
	// Flooded instance does not starve the others; FIFO order preserved.
	if got := ids(backend.FairPick(mk("AAAB"), 10, 1)); got != "AB" {
		t.Fatalf("cap1: %q", got)
	}
	if got := ids(backend.FairPick(mk("AAAB"), 10, 2)); got != "AAB" {
		t.Fatalf("cap2: %q", got)
	}
	// Strict FIFO when disabled or when capacity exhausts the list.
	if got := ids(backend.FairPick(mk("AAAB"), 10, 0)); got != "AAAB" {
		t.Fatalf("off: %q", got)
	}
	if got := ids(backend.FairPick(mk("AAAB"), 2, 1)); got != "AB" {
		t.Fatalf("limit2: %q", got)
	}
	if got := backend.FairPick(mk("AAAB"), 0, 1); len(got) != 0 {
		t.Fatalf("limit0: %v", got)
	}

	// FairPicker must produce exactly the FairPick result when fed the same
	// candidates, including across page boundaries (empty offers between).
	for _, spec := range []string{"AAAB", "AAAAAB", "ABBAAB", "AAAAAAAAAB"} {
		for _, perInst := range []int{1, 2, 3} {
			want := ids(backend.FairPick(mk(spec), 10, perInst))
			p := backend.NewFairPicker(10, perInst)
			for _, r := range mk(spec) {
				if p.Full() {
					t.Fatalf("%s cap%d: full too early", spec, perInst)
				}
				p.Offer(r)
			}
			if got := ids(p.Picked()); got != want {
				t.Fatalf("%s cap%d: picker %q want %q", spec, perInst, got, want)
			}
		}
	}
	// Paging stops as soon as the batch fills.
	p := backend.NewFairPicker(2, 1)
	for _, r := range mk("AAAB") {
		if p.Offer(r) {
			break
		}
	}
	if got := ids(p.Picked()); got != "AB" {
		t.Fatalf("early-stop: %q", got)
	}
	// A refill pass seeded with already-secured rows keeps the per-instance
	// cap across passes: A is already at cap 1, so only B may be added.
	q := backend.NewFairPicker(2, 1)
	q.Seed(p.Picked()[:1])
	for _, r := range mk("AAB") {
		if q.Offer(r) {
			break
		}
	}
	if got := ids(q.Picked()); got != "B" {
		t.Fatalf("seeded refill: %q", got)
	}
	if of := backend.FairOverfetch(1); of < 64 || of <= 1 {
		t.Fatalf("overfetch %d", of)
	}
	if of := backend.FairOverfetch(5000); of < 5000 || of > 8192 {
		t.Fatalf("overfetch %d", of)
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
