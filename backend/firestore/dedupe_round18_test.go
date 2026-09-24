package firestore

import (
	"testing"
)

// TestFramedLegacyDocCollision pins the aliasing behind issue #296 round-18
// P1: the framed doc ID of one (instance, key) pair can equal the legacy doc
// ID of another pair, so a framed probe can land exactly on a foreign legacy
// row. Probing must continue to the legacy candidate after an ownership
// mismatch instead of returning the foreign row.
func TestFramedLegacyDocCollision(t *testing.T) {
	framed := frameDedupeDocID("A", "x")
	legacy := legacyDedupeDocID("1:A", "x")
	if framed != legacy {
		t.Fatalf("expected the colliding framings to alias, got %q vs %q", framed, legacy)
	}
}

// TestMatchOwnedDedupeRow covers the match-aware half of the round-18 P1
// selection: ownership alone is not enough — an owned-but-not-matching
// framed row must not hide an owned matching legacy row.
func TestMatchOwnedDedupeRow(t *testing.T) {
	ownMatch := map[string]any{"instance_id": "A", "dedupe_id": "x", dedupeFormatVersionField: int64(1)}
	ownOther := map[string]any{"instance_id": "A", "dedupe_id": "______x", dedupeFormatVersionField: int64(1)}
	foreign := map[string]any{"instance_id": "1:A", "dedupe_id": "x"}
	legacyOwn := map[string]any{"instance_id": "A", "dedupe_id": "x"}
	cases := []struct {
		name    string
		framed  map[string]any
		legacy  map[string]any
		wantHit bool
	}{
		{"both absent", nil, nil, false},
		{"framed owned match", ownMatch, nil, true},
		{"legacy owned match", nil, legacyOwn, true},
		{"framed owned match wins", ownMatch, legacyOwn, true},
		// Same-instance canonical guard of another ID at the framed doc
		// must not hide this ID's owned legacy guard.
		{"framed owned non-match yields to legacy match", ownOther, legacyOwn, true},
		{"foreign framed yields to legacy match", foreign, legacyOwn, true},
		{"foreign framed alone", foreign, nil, false},
		{"owned non-matches", ownOther, nil, false},
	}
	for _, c := range cases {
		pr := dedupeKeyProbe{framedDoc: c.framed, legacyDoc: c.legacy}
		if got := matchOwnedDedupeRow("x", "x", pr, "A"); got != c.wantHit {
			t.Fatalf("%s: match = %v, want %v", c.name, got, c.wantHit)
		}
	}
}
func TestSelectOwnedDedupeDoc(t *testing.T) {
	own := map[string]any{"instance_id": "A", "dedupe_id": "x"}
	foreign := map[string]any{"instance_id": "1:A", "dedupe_id": "x"}
	cases := []struct {
		name      string
		framed    map[string]any
		legacy    map[string]any
		wantOwned map[string]any
		wantOcc   bool
	}{
		{"both absent", nil, nil, nil, false},
		{"framed owned", own, nil, own, true},
		{"legacy owned", nil, own, own, true},
		{"framed owned wins over legacy owned", own, own, own, true},
		{"foreign framed yields to owned legacy", foreign, own, own, true},
		{"owned framed wins over foreign legacy", own, foreign, own, true},
		{"foreign framed alone", foreign, nil, nil, true},
		{"foreign legacy alone", nil, foreign, nil, true},
		{"both foreign", foreign, foreign, nil, true},
	}
	for _, c := range cases {
		got, occ := selectOwnedDedupeDoc(c.framed, c.legacy, "A")
		if occ != c.wantOcc {
			t.Fatalf("%s: occupied = %v, want %v", c.name, occ, c.wantOcc)
		}
		if c.wantOwned == nil {
			if got != nil {
				t.Fatalf("%s: owned = %v, want nil", c.name, got)
			}
			continue
		}
		if got == nil || got["instance_id"] != "A" {
			t.Fatalf("%s: owned = %v, want the owned row", c.name, got)
		}
	}
}

// TestPickDedupeGuardTarget covers the round-18 guard placement: existing
// preference order (framed canonical, framed fallback) is preserved, the
// legacy framings serve as overflow for foreign-occupied framed docs (P1),
// and batch-reserved docs count as unavailable (P2).
func TestPickDedupeGuardTarget(t *testing.T) {
	const inst = "A"
	canon, fallback := escapeDedupeID("__x"), rawFallbackDedupeKey("__x") // "____x", "__x"
	if canon == fallback {
		t.Fatal("test setup: canonical and fallback keys must differ here")
	}
	t.Run("all free prefers framed canonical", func(t *testing.T) {
		got, ok := pickDedupeGuardTarget(inst, canon, fallback, true, true, true, true, map[string]bool{})
		if !ok || got.docID != frameDedupeDocID(inst, canon) || got.ver != int64(dedupeFormatVersion) {
			t.Fatalf("got %+v %v, want framed canonical v1", got, ok)
		}
	})
	t.Run("occupied canonical falls back to framed fallback", func(t *testing.T) {
		got, ok := pickDedupeGuardTarget(inst, canon, fallback, false, true, true, true, map[string]bool{})
		if !ok || got.docID != frameDedupeDocID(inst, fallback) || got.ver != int64(dedupeFormatRawKeyVersion) {
			t.Fatalf("got %+v %v, want framed fallback v2", got, ok)
		}
	})
	t.Run("legacy-occupied canonical never forks to framed canonical", func(t *testing.T) {
		// Framed canonical doc free but the legacy leg holds a row: the
		// key is occupied, so the guard goes to the framed fallback
		// (round-16/round-17 behavior preserved — see
		// TestDedupeLongFallbackDelivers), never to the framed
		// canonical alongside it.
		got, ok := pickDedupeGuardTarget(inst, canon, fallback, true, false, true, true, map[string]bool{})
		if !ok || got.docID != frameDedupeDocID(inst, fallback) || got.ver != int64(dedupeFormatRawKeyVersion) {
			t.Fatalf("got %+v %v, want framed fallback v2", got, ok)
		}
	})
	t.Run("foreign-occupied framed short ID creates at legacy framing", func(t *testing.T) {
		// DedupeID "x": canonical == fallback == "x", and the framed doc
		// "1:A:x" holds instance "1:A"'s legacy row. The legacy leg "A:x"
		// is free, so the guard is created there (v1 canonical form) —
		// previously nothing was created and every retry appended
		// unguarded.
		got, ok := pickDedupeGuardTarget(inst, "x", "x", false, true, false, true, map[string]bool{})
		if !ok || got.docID != legacyDedupeDocID(inst, "x") || got.ver != int64(dedupeFormatVersion) {
			t.Fatalf("got %+v %v, want legacy canonical v1", got, ok)
		}
	})
	t.Run("reserved canonical moves to fallback", func(t *testing.T) {
		reserved := map[string]bool{frameDedupeDocID(inst, canon): true}
		got, ok := pickDedupeGuardTarget(inst, canon, fallback, true, true, true, true, reserved)
		if !ok || got.docID != frameDedupeDocID(inst, fallback) {
			t.Fatalf("got %+v %v, want framed fallback (canonical reserved)", got, ok)
		}
	})
	t.Run("nothing free inserts unguarded", func(t *testing.T) {
		if _, ok := pickDedupeGuardTarget(inst, canon, fallback, false, false, false, false, map[string]bool{}); ok {
			t.Fatal("fully occupied keys must yield no target (unguarded insert)")
		}
		reserved := map[string]bool{
			frameDedupeDocID(inst, canon):     true,
			frameDedupeDocID(inst, fallback):  true,
			legacyDedupeDocID(inst, canon):    true,
			legacyDedupeDocID(inst, fallback): true,
		}
		if _, ok := pickDedupeGuardTarget(inst, canon, fallback, true, true, true, true, reserved); ok {
			t.Fatal("fully reserved keys must yield no target (unguarded insert)")
		}
	})
}

// TestPickDedupeGuardTargetBatchCollision replays the round-18 P2 batch:
// IDs "____x" + "__x" with foreign-occupied "______x". The first item falls
// back to "____x"; the second item's canonical probe cannot see the buffered
// Create, so without the reservation both would choose the same doc and the
// whole batch would fail deterministically on every retry.
func TestPickDedupeGuardTargetBatchCollision(t *testing.T) {
	const inst = "I"
	first := "____x" // canonical "______x" (foreign-occupied), fallback "____x"
	second := "__x"  // canonical "____x", fallback "__x"
	canon1, fb1 := escapeDedupeID(first), rawFallbackDedupeKey(first)
	canon2, fb2 := escapeDedupeID(second), rawFallbackDedupeKey(second)
	if canon1 != "______x" || fb1 != "____x" || canon2 != "____x" || fb2 != "__x" {
		t.Fatalf("test setup keys changed: %q %q %q %q", canon1, fb1, canon2, fb2)
	}
	reserved := map[string]bool{}
	// Item 1: framed canonical occupied (foreign), framed fallback free.
	t1, ok := pickDedupeGuardTarget(inst, canon1, fb1, false, true, true, true, reserved)
	if !ok {
		t.Fatal("item 1 must find its fallback slot")
	}
	reserved[t1.docID] = true
	// Item 2: its canonical doc reads free in-txn (item 1's Create is
	// buffered and invisible) but is reserved by item 1.
	t2, ok := pickDedupeGuardTarget(inst, canon2, fb2, true, true, true, true, reserved)
	if !ok {
		t.Fatal("item 2 must find a distinct slot")
	}
	if t1.docID == t2.docID {
		t.Fatalf("both items chose %q: the batch would fail deterministically", t1.docID)
	}
	if t1.docID != frameDedupeDocID(inst, "____x") || t2.docID != frameDedupeDocID(inst, "__x") {
		t.Fatalf("got %q and %q, want distinct fallback docs", t1.docID, t2.docID)
	}
}
