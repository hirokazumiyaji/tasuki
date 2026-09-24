package firestore

import (
	"testing"
)

// Rolling-upgrade dual-write (Codex round-24 P1 on #296): new guards are
// written under the length-framed doc ID, but pre-framing nodes probe only
// instanceID + ":" + key. A retry routed to an old node misses the
// framed-only guard and duplicates the inbox event — even for ordinary IDs
// with no colons. New code dual-writes the legacy-format counterpart so old
// readers see the guard; new readers probe framed-then-legacy as before.
func TestDualDedupeGuardDocCoversOldReader(t *testing.T) {
	const inst = "roll-upg"
	for _, dedupe := range []string{"k", "ordinary-id", "a:b:c", "__x", "____x"} {
		canon, fallback := escapeDedupeID(dedupe), rawFallbackDedupeKey(dedupe)
		target, ok := pickDedupeGuardTarget(inst, dedupe, canon, fallback, freeProbe(), freeProbe(), map[string]bool{})
		if !ok {
			t.Fatalf("%q: no guard target on empty store", dedupe)
		}
		dual, ok := dualDedupeGuardDoc(inst, target, canon, fallback, true, true, map[string]bool{})
		if !ok {
			t.Fatalf("%q: framed guard %q has no legacy counterpart (old reader would miss it)", dedupe, target.docID)
		}
		wantLegacy := legacyDedupeDocID(inst, canon)
		// When canonical == fallback (short plain IDs) both framings share
		// one doc; the counterpart is still the legacy canonical doc.
		if dual.docID != wantLegacy {
			t.Fatalf("%q: dual = %q, want legacy %q", dedupe, dual.docID, wantLegacy)
		}
		if dual.ver != target.ver {
			t.Fatalf("%q: dual version %d != primary %d (stamp travels with the key)", dedupe, dual.ver, target.ver)
		}
		// Old-format reader simulation: it reads ONLY the legacy doc ID
		// and validates ownership + version. Both rows must match the
		// requested ID identically.
		framedRow := map[string]any{"instance_id": inst, "dedupe_id": canon, dedupeFormatVersionField: target.ver}
		legacyRow := map[string]any{"instance_id": inst, "dedupe_id": canon, dedupeFormatVersionField: dual.ver}
		if !docInstanceMatches(legacyRow, inst) || !matchDedupeRow(dedupe, canon, legacyRow) {
			t.Fatalf("%q: old-format reader misses the legacy duplicate", dedupe)
		}
		if !docInstanceMatches(framedRow, inst) || !matchDedupeRow(dedupe, canon, framedRow) {
			t.Fatalf("%q: new reader misses the framed primary", dedupe)
		}
		// Fail-without-fix pin: the framed-only write of the old code is
		// invisible at the legacy doc ID by construction (distinct IDs for
		// ordinary instances), so an old reader probing only the legacy
		// form finds nothing.
		if target.docID == dual.docID {
			t.Fatalf("%q: primary and dual alias (%q); dual-write is a no-op", dedupe, target.docID)
		}
		if target.docID == legacyDedupeDocID(inst, canon) {
			t.Fatalf("%q: primary already legacy; nothing to dual-write", dedupe)
		}
	}
}

func TestDualDedupeGuardDocNoCounterpartWhenLegacy(t *testing.T) {
	const inst = "A"
	// Legacy overflow target (framed occupied): old readers see it directly.
	legacy := dedupeGuardTarget{docID: legacyDedupeDocID(inst, "x"), ver: int64(dedupeFormatVersion)}
	if _, ok := dualDedupeGuardDoc(inst, legacy, "x", "x", true, true, map[string]bool{}); ok {
		t.Fatal("legacy target must not gain a counterpart")
	}
	// Occupied legacy leg: no counterpart (would collide).
	framed := dedupeGuardTarget{docID: frameDedupeDocID(inst, "x"), ver: int64(dedupeFormatVersion)}
	if _, ok := dualDedupeGuardDoc(inst, framed, "x", "x", false, true, map[string]bool{}); ok {
		t.Fatal("occupied legacy leg must yield no counterpart")
	}
	// Reserved legacy doc (same-batch race): no counterpart.
	reserved := map[string]bool{legacyDedupeDocID(inst, "x"): true}
	if _, ok := dualDedupeGuardDoc(inst, framed, "x", "x", true, true, reserved); ok {
		t.Fatal("reserved legacy doc must yield no counterpart")
	}
	// Fallback keys dual-write the fallback legacy form, not canonical.
	canon, fallback := escapeDedupeID("__x"), rawFallbackDedupeKey("__x")
	fbTarget := dedupeGuardTarget{docID: frameDedupeDocID(inst, fallback), ver: int64(dedupeFormatRawKeyVersion)}
	dual, ok := dualDedupeGuardDoc(inst, fbTarget, canon, fallback, true, true, map[string]bool{})
	if !ok || dual.docID != legacyDedupeDocID(inst, fallback) || dual.ver != int64(dedupeFormatRawKeyVersion) {
		t.Fatalf("fallback dual = %+v %v, want legacy fallback v2", dual, ok)
	}
}

func TestMarkerGuardTargetAndDual(t *testing.T) {
	const inst = "roll-m"
	const dedupe = "k"
	framed := postTerminalMarkerDocID(inst, dedupe)
	legacy := legacyDedupeDocID(inst, postTerminalDedupeMarker(dedupe))
	if framed == legacy {
		t.Fatal("test setup: framed and legacy marker docs must differ")
	}
	// Both free: primary framed, dual legacy.
	got, ok := markerGuardTarget(inst, dedupe, true, true, map[string]bool{})
	if !ok || got != framed {
		t.Fatalf("marker target = %q %v, want framed %q", got, ok, framed)
	}
	dual, ok := dualMarkerDoc(inst, dedupe, true, map[string]bool{})
	if !ok || dual != legacy {
		t.Fatalf("marker dual = %q %v, want legacy %q", dual, ok, legacy)
	}
	// Framed foreign-occupied: fall back to legacy alone (old code failed
	// the send deterministically on every retry with an AlreadyExists
	// Create; the legacy leg is probed by new readers and seen directly by
	// old ones).
	got, ok = markerGuardTarget(inst, dedupe, false, true, map[string]bool{})
	if !ok || got != legacy {
		t.Fatalf("occupied framed marker target = %q %v, want legacy %q", got, ok, legacy)
	}
	// Both occupied: no marker (event still inserts; retry may duplicate
	// once rather than the send failing).
	if _, ok := markerGuardTarget(inst, dedupe, false, false, map[string]bool{}); ok {
		t.Fatal("fully occupied marker must yield no target")
	}
	// Reserved framed: legacy fallback.
	reserved := map[string]bool{framed: true}
	got, ok = markerGuardTarget(inst, dedupe, true, true, reserved)
	if !ok || got != legacy {
		t.Fatalf("reserved framed marker target = %q %v, want legacy %q", got, ok, legacy)
	}
	// Occupied/reserved legacy: no dual.
	if _, ok := dualMarkerDoc(inst, dedupe, false, map[string]bool{}); ok {
		t.Fatal("occupied legacy marker must yield no dual")
	}
	if _, ok := dualMarkerDoc(inst, dedupe, true, map[string]bool{legacy: true}); ok {
		t.Fatal("reserved legacy marker must yield no dual")
	}
}
