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
	p := backend.NewFairPicker(2, 1).TrackRejected()
	for _, r := range mk("AAAB") {
		if p.Offer(r) {
			break
		}
	}
	if got := ids(p.Picked()); got != "AB" {
		t.Fatalf("early-stop: %q", got)
	}
	if got := ids(p.Rejected()); got != "AA" {
		t.Fatalf("rejected: %q want %q", got, "AA")
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
	// Losing both picks (A,B) frees the cap for the rejected A: revisiting
	// the rejected row must yield it.
	r := backend.NewFairPicker(2, 1)
	for _, ref := range p.Rejected() {
		r.Offer(ref)
	}
	if got := ids(r.Picked()); got != "A" {
		t.Fatalf("revisit rejected: %q want %q", got, "A")
	}
	if of := backend.FairOverfetch(1); of < 64 || of <= 1 {
		t.Fatalf("overfetch %d", of)
	}
	if of := backend.FairOverfetch(5000); of < 5000 || of > 8192 {
		t.Fatalf("overfetch %d", of)
	}
}

func TestFairPickerRejectedOptIn(t *testing.T) {
	mk := func(spec string) []backend.FairTaskRef {
		var out []backend.FairTaskRef
		for i, c := range []byte(spec) {
			out = append(out, backend.FairTaskRef{ID: int64(i + 1), InstanceID: string(c)})
		}
		return out
	}
	// Default off: over-cap candidates are dropped without accumulating,
	// so single-pass callers (SQLite) pay O(1) extra memory.
	off := backend.NewFairPicker(2, 1)
	for _, r := range mk("AAAB") {
		if off.Offer(r) {
			break
		}
	}
	if got := off.Rejected(); len(got) != 0 {
		t.Fatalf("default Rejected = %v, want empty (opt-in required)", got)
	}
	// Opted in: the same feed retains the over-cap rows.
	on := backend.NewFairPicker(2, 1).TrackRejected()
	for _, r := range mk("AAAB") {
		if on.Offer(r) {
			break
		}
	}
	if got := len(on.Rejected()); got != 2 {
		t.Fatalf("tracked Rejected len = %d, want 2", got)
	}
}

func TestFairPickerRejectedCap(t *testing.T) {
	// Retention is bounded: a flood-sized backlog must not accumulate O(queue)
	// refs in the picker or the cross-pass carry.
	p := backend.NewFairPicker(2, 1).TrackRejected()
	const flood = backend.FairRejectedCap + 500
	full := false
	for i := 0; i < flood+2; i++ {
		inst := "A"
		if i >= flood+1 {
			inst = "B"
		}
		if p.Offer(backend.FairTaskRef{ID: int64(i + 1), InstanceID: inst}) {
			full = true
			break
		}
	}
	if !full {
		t.Fatal("want the batch to fill via B once offered")
	}
	if got := len(p.Rejected()); got != backend.FairRejectedCap {
		t.Fatalf("Rejected len = %d, want cap %d", got, backend.FairRejectedCap)
	}
	if !p.RejectedCapped() {
		t.Fatal("want RejectedCapped after overflowing the carry bound")
	}
	// Below the cap nothing is dropped and the flag stays clear.
	q := backend.NewFairPicker(2, 1).TrackRejected()
	for _, r := range []backend.FairTaskRef{{ID: 1, InstanceID: "A"}, {ID: 2, InstanceID: "A"}, {ID: 3, InstanceID: "B"}} {
		if q.Offer(r) {
			break
		}
	}
	if len(q.Rejected()) != 1 || q.RejectedCapped() {
		t.Fatalf("below-cap Rejected = %v capped=%v, want 1 row and no cap", q.Rejected(), q.RejectedCapped())
	}
	// Default-off pickers never retain and never report the cap.
	off := backend.NewFairPicker(2, 1)
	for i := 0; i < flood; i++ {
		if off.Offer(backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A"}) {
			break
		}
	}
	if len(off.Rejected()) != 0 || off.RejectedCapped() {
		t.Fatalf("opt-out Rejected = %v capped=%v, want empty", off.Rejected(), off.RejectedCapped())
	}
}

func TestFairPickerScanContinuesPastRejectedCap(t *testing.T) {
	// Covers the issue #294 P1 follow-up at the FairPicker level: a flood
	// that overflows rejected retention must not end the scan — the cap
	// bounds the carry list, not the scan. (Live-DB cap-overflow coverage
	// would need FairRejectedCap+ rows; the postgres/mysql candidate loops
	// mirror scanContinue below: page FIFO candidates, offer each, stop only
	// on Full or end-of-candidates, never on RejectedCapped.)
	const page = 64
	scanContinue := func(feed []backend.FairTaskRef, limit, perInstance int) *backend.FairPicker {
		p := backend.NewFairPicker(limit, perInstance).TrackRejected()
		for i := 0; i < len(feed) && !p.Full(); {
			next := i + page
			if next > len(feed) {
				next = len(feed)
			}
			for _, r := range feed[i:next] {
				if p.Offer(r) {
					break
				}
			}
			i = next
		}
		return p
	}
	// scanWithCapBreak mirrors the pre-fix loop, which stopped paging once
	// rejected retention hit the cap, stranding the unscanned tail.
	scanWithCapBreak := func(feed []backend.FairTaskRef, limit, perInstance int) *backend.FairPicker {
		p := backend.NewFairPicker(limit, perInstance).TrackRejected()
		for i := 0; i < len(feed) && !p.Full(); {
			next := i + page
			if next > len(feed) {
				next = len(feed)
			}
			for _, r := range feed[i:next] {
				if p.Offer(r) {
					break
				}
				if p.RejectedCapped() {
					break
				}
			}
			if p.RejectedCapped() {
				break
			}
			i = next
		}
		return p
	}

	// FIFO: one A pick, then an A flood overflowing retention, then victim B.
	var feed []backend.FairTaskRef
	feed = append(feed, backend.FairTaskRef{ID: 1, InstanceID: "A"})
	for i := 0; i < backend.FairRejectedCap+500; i++ {
		feed = append(feed, backend.FairTaskRef{ID: int64(2 + i), InstanceID: "A"})
	}
	feed = append(feed, backend.FairTaskRef{ID: int64(len(feed) + 1), InstanceID: "B"})

	// Pre-fix shape: the scan stops at the cap with only A1 picked; B is
	// never reached in this pass.
	old := scanWithCapBreak(feed, 2, 1)
	if len(old.Picked()) != 1 || len(old.Rejected()) != backend.FairRejectedCap {
		t.Fatalf("pre-fix loop: picked=%v rejected=%d, want 1 pick and a full carry", old.Picked(), len(old.Rejected()))
	}
	for _, r := range old.Picked() {
		if r.InstanceID == "B" {
			t.Fatal("pre-fix loop unexpectedly reached B")
		}
	}

	// Fixed contract: the same feed fills the batch with A1,B1, retention
	// stays bounded, and the overflow is reported but does not stop the scan.
	got := scanContinue(feed, 2, 1)
	if len(got.Picked()) != 2 {
		t.Fatalf("continuing scan: picked=%v, want [A B]", got.Picked())
	}
	if got.Picked()[0].InstanceID != "A" || got.Picked()[1].InstanceID != "B" {
		t.Fatalf("continuing scan: picked=%v, want victim B in the same pass", got.Picked())
	}
	if len(got.Rejected()) != backend.FairRejectedCap || !got.RejectedCapped() {
		t.Fatalf("continuing scan: rejected=%d capped=%v, want %d and capped",
			len(got.Rejected()), got.RejectedCapped(), backend.FairRejectedCap)
	}

	// Refill starvation shape from the finding: pass 1 (pre-fix) claims only
	// A1 and carries 2000 retained As with the cursor mid-flood. The refill
	// seeds A1, re-offers the retained As (all rejected again, carry full),
	// then scans a tail of more flood As plus a later victim B2.
	tail := []backend.FairTaskRef{{ID: 1e9, InstanceID: "A"}, {ID: 1e9 + 1, InstanceID: "B"}}
	refillOld := backend.NewFairPicker(1, 1).TrackRejected()
	refillOld.Seed(old.Picked())
	for _, r := range old.Rejected() {
		refillOld.Offer(r)
	}
	for _, r := range tail {
		if refillOld.Offer(r) {
			break
		}
		if refillOld.RejectedCapped() {
			break
		}
	}
	if len(refillOld.Picked()) != 0 {
		t.Fatalf("pre-fix refill: picked=%v, want empty (caps with no picks)", refillOld.Picked())
	}
	refillFixed := backend.NewFairPicker(1, 1).TrackRejected()
	refillFixed.Seed(old.Picked())
	for _, r := range old.Rejected() {
		refillFixed.Offer(r)
	}
	for _, r := range tail {
		if refillFixed.Offer(r) {
			break
		}
	}
	if len(refillFixed.Picked()) != 1 || refillFixed.Picked()[0].InstanceID != "B" {
		t.Fatalf("continuing refill: picked=%v, want the later victim B2", refillFixed.Picked())
	}
	if len(refillFixed.Rejected()) != backend.FairRejectedCap {
		t.Fatalf("continuing refill: rejected=%d, want bounded carry %d",
			len(refillFixed.Rejected()), backend.FairRejectedCap)
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
