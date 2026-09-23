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

func TestFairOverflowDropsPastCap(t *testing.T) {
	// Premise for the refill requery (issue #294 P2): with Limit=2 and
	// MaxPerInstance=1 over FIFO A1..A2002, one pass picks A1, retains
	// A2..A2001 (bounded by FairRejectedCap), and drops A2002 while reporting
	// the overflow — the scan cursor has advanced past A2002, so only a
	// requery from the pre-overflow position can recover it within the claim.
	p := backend.NewFairPicker(2, 1).TrackRejected()
	total := backend.FairRejectedCap + 2
	for i := 1; i <= total; i++ {
		p.Offer(backend.FairTaskRef{ID: int64(i), InstanceID: "A"})
	}
	if len(p.Picked()) != 1 || p.Picked()[0].ID != 1 {
		t.Fatalf("picked=%v, want [A1]", p.Picked())
	}
	if len(p.Rejected()) != backend.FairRejectedCap || !p.RejectedCapped() {
		t.Fatalf("rejected=%d capped=%v, want %d and capped",
			len(p.Rejected()), p.RejectedCapped(), backend.FairRejectedCap)
	}
	for _, r := range p.Rejected() {
		if r.ID == int64(total) {
			t.Fatalf("A%d must be dropped past the cap, not retained", total)
		}
	}
}

func TestFairOverflowRequeryMirror(t *testing.T) {
	// Covers the issue #294 P2 at the refill-loop level without a live DB:
	// Limit=2, MaxPerInstance=1 over FIFO A1..A2002 (FairRejectedCap+2 rows
	// from one instance) with A1..A2001 lost to concurrent locks. The first
	// pass picks A1, retains A2..A2001, and drops A2002 past FairRejectedCap;
	// the refill then chews through the retained carry one lost pick per
	// pass. runRefill mirrors the postgres/mysql refill loops (pending carry
	// with FairRejectedCap bound, monotonic keyset cursor, lock step,
	// pre-overflow cursor snapshot, attempted-ID skip); with requery disabled
	// it returns empty (the reported bug), with the single bounded requery
	// pass it recovers the never-attempted A2002.
	const limit, perInstance = 2, 1
	total := backend.FairRejectedCap + 2
	feed := make([]backend.FairTaskRef, 0, total)
	for i := 0; i < total; i++ {
		feed = append(feed, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A"})
	}
	// lockLost mirrors losing the SKIP LOCKED race: every row the claim can
	// reach except the overflow-dropped tail is contended.
	lockLost := func(id int64) bool { return id <= int64(total-1) }

	runRefill := func(requery bool) []backend.FairTaskRef {
		cursor := 0 // index of the next unscanned feed row (keyset cursor)
		var lastID int64
		first := true
		var out, claimed, pending []backend.FairTaskRef
		var overflowSnapValid, overflowSeen, requeryDone bool
		var overflowSnapID int64
		attempted := map[int64]struct{}{}
		passes := 0
		startOverflowRequery := func() bool {
			if len(out) != 0 || !overflowSeen || !overflowSnapValid || requeryDone {
				return false
			}
			requeryDone = true
			first = false
			cursor = int(overflowSnapID) // IDs are 1-based sequential
			pending = nil
			return true
		}
		for len(out) < limit {
			passes++
			if passes > 2*total+10 {
				t.Fatalf("refill loop did not terminate (requery=%v)", requery)
			}
			picker := backend.NewFairPicker(limit-len(out), perInstance).TrackRejected()
			picker.Seed(claimed)
			offered := 0
			for _, r := range pending {
				if picker.Full() {
					break
				}
				picker.Offer(r)
				offered++
			}
			if !picker.Full() {
				for !picker.Full() && cursor < len(feed) {
					r := feed[cursor]
					cursor++
					if !overflowSeen && !picker.RejectedCapped() {
						overflowSnapID, overflowSnapValid = lastID, !first
					}
					first = false
					lastID = r.ID
					if _, dup := attempted[r.ID]; !(requery && requeryDone && dup) {
						offerFull := picker.Offer(r)
						if picker.RejectedCapped() {
							overflowSeen = true
						}
						if offerFull {
							break
						}
					}
				}
			}
			scanExhausted := !picker.Full()
			picked := picker.Picked()
			iterRejected := picker.Rejected()
			for _, r := range picked {
				attempted[r.ID] = struct{}{}
			}
			if len(picked) == 0 {
				if requery && startOverflowRequery() {
					continue
				}
				break
			}
			prevOut := len(out)
			for _, r := range picked {
				if lockLost(r.ID) {
					continue
				}
				out = append(out, r)
				claimed = append(claimed, r)
			}
			if len(out) >= limit {
				break
			}
			if scanExhausted {
				if lost := len(picked) - (len(out) - prevOut); lost == 0 || len(iterRejected) == 0 {
					if requery && startOverflowRequery() {
						continue
					}
					break
				}
			}
			pending = append(iterRejected, pending[offered:]...)
			if len(pending) > backend.FairRejectedCap {
				pending = pending[:backend.FairRejectedCap]
			}
		}
		return out
	}

	if got := runRefill(false); len(got) != 0 {
		t.Fatalf("pre-fix refill: got %v, want empty (overflow-dropped tail unreachable)", got)
	}
	got := runRefill(true)
	if len(got) != 1 || got[0].ID != int64(total) || got[0].InstanceID != "A" {
		t.Fatalf("requery refill: got %v, want [A%d]", got, total)
	}
}

func TestFairOverflowRequeryUnderfilled(t *testing.T) {
	// Covers the round-7 P2: the overflow requery must fire whenever the
	// batch is underfilled, not just when it is empty. Limit=2,
	// MaxPerInstance=1 over FIFO A1..A2002/B1 with A1..A2001 lost to
	// concurrent locks: pass 1 secures B1 while the retained As drain on
	// locks, and only a requery from the pre-overflow snapshot recovers the
	// dropped A2002 for slot 2. Empty-guarded requeries return [B1]; the
	// underfilled guard returns [B1 A2002].
	const limit, perInstance = 2, 1
	aTotal := backend.FairRejectedCap + 2
	feed := make([]backend.FairTaskRef, 0, aTotal+1)
	for i := 0; i < aTotal; i++ {
		feed = append(feed, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A"})
	}
	feed = append(feed, backend.FairTaskRef{ID: int64(aTotal + 1), InstanceID: "B"})
	// Every retained row is contended; only the dropped A2002 and B1 win.
	lockLost := func(id int64) bool { return id <= int64(aTotal-1) }

	runRefill := func(guard string) []backend.FairTaskRef {
		cursor := 0
		var lastID int64
		first := true
		var out, claimed, pending []backend.FairTaskRef
		var overflowSnapValid, overflowSeen, requeryDone bool
		var overflowSnapID int64
		attempted := map[int64]struct{}{}
		passes := 0
		allowRequery := func() bool {
			switch guard {
			case "underfilled":
				if len(out) >= limit {
					return false
				}
			case "empty":
				if len(out) != 0 {
					return false
				}
			default:
				return false
			}
			return overflowSeen && overflowSnapValid && !requeryDone
		}
		startOverflowRequery := func() bool {
			if !allowRequery() {
				return false
			}
			requeryDone = true
			first = false
			cursor = int(overflowSnapID)
			pending = nil
			return true
		}
		for len(out) < limit {
			passes++
			if passes > 2*len(feed)+10 {
				t.Fatalf("refill loop did not terminate (guard=%s)", guard)
			}
			picker := backend.NewFairPicker(limit-len(out), perInstance).TrackRejected()
			picker.Seed(claimed)
			offered := 0
			for _, r := range pending {
				if picker.Full() {
					break
				}
				picker.Offer(r)
				offered++
			}
			if !picker.Full() {
				for !picker.Full() && cursor < len(feed) {
					r := feed[cursor]
					cursor++
					if !overflowSeen && !picker.RejectedCapped() {
						overflowSnapID, overflowSnapValid = lastID, !first
					}
					first = false
					lastID = r.ID
					if _, dup := attempted[r.ID]; !(requeryDone && dup) {
						offerFull := picker.Offer(r)
						if picker.RejectedCapped() {
							overflowSeen = true
						}
						if offerFull {
							break
						}
					}
				}
			}
			scanExhausted := !picker.Full()
			picked := picker.Picked()
			iterRejected := picker.Rejected()
			for _, r := range picked {
				attempted[r.ID] = struct{}{}
			}
			if len(picked) == 0 {
				if startOverflowRequery() {
					continue
				}
				break
			}
			prevOut := len(out)
			for _, r := range picked {
				if lockLost(r.ID) {
					continue
				}
				out = append(out, r)
				claimed = append(claimed, r)
			}
			if len(out) >= limit {
				break
			}
			if scanExhausted {
				if lost := len(picked) - (len(out) - prevOut); lost == 0 || len(iterRejected) == 0 {
					if startOverflowRequery() {
						continue
					}
					break
				}
			}
			pending = append(iterRejected, pending[offered:]...)
			if len(pending) > backend.FairRejectedCap {
				pending = pending[:backend.FairRejectedCap]
			}
		}
		return out
	}

	if got := runRefill("none"); len(got) != 1 || got[0].InstanceID != "B" {
		t.Fatalf("no-requery refill: got %v, want [B1] (dropped A2002 unreachable)", got)
	}
	if got := runRefill("empty"); len(got) != 1 || got[0].InstanceID != "B" {
		t.Fatalf("empty-guarded requery: got %v, want [B1] (reproduces the underfill bug)", got)
	}
	got := runRefill("underfilled")
	if len(got) != 2 {
		t.Fatalf("underfilled-guarded requery: got %v, want [B1 A2002]", got)
	}
	seen := map[int64]bool{}
	for _, r := range got {
		seen[r.ID] = true
	}
	if !seen[int64(aTotal)] || !seen[int64(aTotal+1)] {
		t.Fatalf("underfilled-guarded requery: got %v, want B1 and dropped A%d", got, aTotal)
	}
}

func TestFairOverflowRequerySuccessiveSegments(t *testing.T) {
	// Covers the round-8 P2 on the postgres/mysql refill loops: a flood
	// spanning MULTIPLE rejected-retention caps strands candidates even with
	// the single overflow requery. Limit=2, MaxPerInstance=1 over FIFO
	// A1..A4003 (2*FairRejectedCap+3 rows from one instance) with A1..A4002
	// lost to concurrent locks: the first requery advances one segment
	// (~2001 rows) and drops the tail past the cap again, so the single-shot
	// guard returns empty while the never-attempted A4003 stays claimable.
	// Looping the bounded requery through successive snapshots recovers it.
	const limit, perInstance = 2, 1
	total := 2*backend.FairRejectedCap + 3
	feed := make([]backend.FairTaskRef, 0, total)
	for i := 0; i < total; i++ {
		feed = append(feed, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A"})
	}
	// Only the overflow-dropped tail ever wins the lock race.
	lockLost := func(id int64) bool { return id <= int64(total-1) }

	// runRefill mirrors the postgres/mysql refill loops with the round-8
	// looping requery: successive segments resume from each pass's fresh
	// pre-overflow snapshot while skipping attempted IDs, stop early when a
	// segment makes no progress, and never exceed maxRequeries arms.
	// maxRequeries=1 reproduces the single-shot (pre-fix) behavior.
	runRefill := func(maxRequeries int) (out []backend.FairTaskRef, arms, passes int) {
		cursor := 0 // index of the next unscanned feed row (keyset cursor)
		var lastID int64
		first := true
		var claimed, pending []backend.FairTaskRef
		var overflowSnapValid, overflowSeen bool
		var overflowSnapID int64
		var requeryPasses, requeryAttempted, requeryOut int
		attempted := map[int64]struct{}{}
		startOverflowRequery := func() bool {
			if len(out) >= limit || !overflowSeen || !overflowSnapValid || requeryPasses >= maxRequeries {
				return false
			}
			if requeryPasses > 0 && len(attempted) <= requeryAttempted && len(out) <= requeryOut {
				return false
			}
			requeryPasses++
			arms++
			requeryAttempted, requeryOut = len(attempted), len(out)
			overflowSeen = false
			first = false
			cursor = int(overflowSnapID) // IDs are 1-based sequential
			overflowSnapValid = false
			pending = nil
			return true
		}
		for len(out) < limit {
			passes++
			if passes > 4*len(feed)+20 {
				t.Fatalf("refill loop did not terminate (maxRequeries=%d)", maxRequeries)
			}
			picker := backend.NewFairPicker(limit-len(out), perInstance).TrackRejected()
			picker.Seed(claimed)
			offered := 0
			for _, r := range pending {
				if picker.Full() {
					break
				}
				picker.Offer(r)
				offered++
			}
			if !picker.Full() {
				for !picker.Full() && cursor < len(feed) {
					r := feed[cursor]
					cursor++
					if !overflowSeen && !picker.RejectedCapped() {
						overflowSnapID, overflowSnapValid = lastID, !first
					}
					first = false
					lastID = r.ID
					if _, dup := attempted[r.ID]; !(requeryPasses > 0 && dup) {
						offerFull := picker.Offer(r)
						if picker.RejectedCapped() {
							overflowSeen = true
						}
						if offerFull {
							break
						}
					}
				}
			}
			scanExhausted := !picker.Full()
			picked := picker.Picked()
			iterRejected := picker.Rejected()
			for _, r := range picked {
				attempted[r.ID] = struct{}{}
			}
			if len(picked) == 0 {
				if startOverflowRequery() {
					continue
				}
				break
			}
			prevOut := len(out)
			for _, r := range picked {
				if lockLost(r.ID) {
					continue
				}
				out = append(out, r)
				claimed = append(claimed, r)
			}
			if len(out) >= limit {
				break
			}
			if scanExhausted {
				if lost := len(picked) - (len(out) - prevOut); lost == 0 || len(iterRejected) == 0 {
					if startOverflowRequery() {
						continue
					}
					break
				}
			}
			pending = append(iterRejected, pending[offered:]...)
			if len(pending) > backend.FairRejectedCap {
				pending = pending[:backend.FairRejectedCap]
			}
		}
		return out, arms, passes
	}

	// Pre-fix shape: the single requery advances one segment, the tail drops
	// past the cap again, the guard blocks any further requery, and the
	// claim returns empty although A4003 was never attempted.
	if got, arms, _ := runRefill(1); len(got) != 0 || arms != 1 {
		t.Fatalf("single-shot requery: got %v arms=%d, want empty with 1 arm (reproduces the bug)", got, arms)
	}
	// Fixed contract: successive segments walk the flood and recover the
	// never-attempted tail with exactly two requery arms.
	got, arms, _ := runRefill(backend.MaxOverflowRequeryPasses)
	if len(got) != 1 || got[0].ID != int64(total) || got[0].InstanceID != "A" {
		t.Fatalf("looping requery: got %v, want [A%d]", got, total)
	}
	if arms != 2 {
		t.Fatalf("looping requery: arms=%d, want 2 successive segments", arms)
	}
}

func TestFairOverflowRequeryNoProgressStops(t *testing.T) {
	// The looping requery must not burn through its pass bound when a
	// segment cannot make progress: Limit=2, MaxPerInstance=1 over FIFO
	// A1..A4003 where only A1 wins the lock race. Pass 1 secures A1; the
	// requery segment then offers only A rows against the seeded per-instance
	// cap, picks nothing, attempts nothing new, and secures nothing — so no
	// second segment may arm even though it overflowed again.
	const limit, perInstance = 2, 1
	total := 2*backend.FairRejectedCap + 3
	feed := make([]backend.FairTaskRef, 0, total)
	for i := 0; i < total; i++ {
		feed = append(feed, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A"})
	}
	lockLost := func(id int64) bool { return id != 1 }

	cursor := 0
	var lastID int64
	first := true
	var out, claimed, pending []backend.FairTaskRef
	var overflowSnapValid, overflowSeen bool
	var overflowSnapID int64
	var requeryPasses, requeryAttempted, requeryOut int
	arms := 0
	attempted := map[int64]struct{}{}
	startOverflowRequery := func() bool {
		if len(out) >= limit || !overflowSeen || !overflowSnapValid || requeryPasses >= backend.MaxOverflowRequeryPasses {
			return false
		}
		if requeryPasses > 0 && len(attempted) <= requeryAttempted && len(out) <= requeryOut {
			return false
		}
		requeryPasses++
		arms++
		requeryAttempted, requeryOut = len(attempted), len(out)
		overflowSeen = false
		first = false
		cursor = int(overflowSnapID)
		overflowSnapValid = false
		pending = nil
		return true
	}
	passes := 0
	for len(out) < limit {
		passes++
		if passes > 4*len(feed)+20 {
			t.Fatal("refill loop did not terminate")
		}
		picker := backend.NewFairPicker(limit-len(out), perInstance).TrackRejected()
		picker.Seed(claimed)
		offered := 0
		for _, r := range pending {
			if picker.Full() {
				break
			}
			picker.Offer(r)
			offered++
		}
		if !picker.Full() {
			for !picker.Full() && cursor < len(feed) {
				r := feed[cursor]
				cursor++
				if !overflowSeen && !picker.RejectedCapped() {
					overflowSnapID, overflowSnapValid = lastID, !first
				}
				first = false
				lastID = r.ID
				if _, dup := attempted[r.ID]; !(requeryPasses > 0 && dup) {
					offerFull := picker.Offer(r)
					if picker.RejectedCapped() {
						overflowSeen = true
					}
					if offerFull {
						break
					}
				}
			}
		}
		scanExhausted := !picker.Full()
		picked := picker.Picked()
		iterRejected := picker.Rejected()
		for _, r := range picked {
			attempted[r.ID] = struct{}{}
		}
		if len(picked) == 0 {
			if startOverflowRequery() {
				continue
			}
			break
		}
		prevOut := len(out)
		for _, r := range picked {
			if lockLost(r.ID) {
				continue
			}
			out = append(out, r)
			claimed = append(claimed, r)
		}
		if len(out) >= limit {
			break
		}
		if scanExhausted {
			if lost := len(picked) - (len(out) - prevOut); lost == 0 || len(iterRejected) == 0 {
				if startOverflowRequery() {
					continue
				}
				break
			}
		}
		pending = append(iterRejected, pending[offered:]...)
		if len(pending) > backend.FairRejectedCap {
			pending = pending[:backend.FairRejectedCap]
		}
	}

	if len(out) != 1 || out[0].ID != 1 {
		t.Fatalf("no-progress refill: got %v, want [A1]", out)
	}
	if arms != 1 {
		t.Fatalf("no-progress refill: arms=%d, want exactly 1 (second segment must stop for no progress)", arms)
	}
}

func TestFairPickerRelease(t *testing.T) {
	mk := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst}
	}
	p := backend.NewFairPicker(2, 1)
	p.Offer(mk(1, "a"))
	p.Offer(mk(2, "b"))
	if !p.Full() {
		t.Fatal("want full")
	}
	// Unknown IDs are a no-op.
	if p.Release(mk(99, "z")) {
		t.Fatal("unknown release must return false")
	}
	if !p.Full() {
		t.Fatal("no-op release must not change fullness")
	}
	// A lost claim race frees its slot: Full clears and the same instance
	// may be picked again by a later queue.
	if !p.Release(mk(1, "a")) {
		t.Fatal("want release")
	}
	if p.Full() {
		t.Fatal("want room after release")
	}
	if full := p.Offer(mk(3, "a")); !full {
		t.Fatal("replacement pick should fill the batch")
	}
	got := p.Picked()
	if len(got) != 2 || got[0].ID != 2 || got[1].ID != 3 {
		t.Fatalf("picked=%v", got)
	}
	// Double release removes at most one entry.
	if !p.Release(mk(2, "b")) || p.Release(mk(2, "b")) {
		t.Fatal("second release of the same ID must return false")
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
