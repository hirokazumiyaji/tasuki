package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestTrimFairCarryDropsViableTail documents issue #294 round-19 P1 against
// the fixed quota+margin window: Limit=2/MaxPerInstance=1 over A1..A68/B1
// with A1..A67 locked. Pass 1 secures B1 and carries A2..A68; the plain trim
// keeps A2..A66 (1 quota + 64 margin) and permanently forgets A67,A68 before
// either is attempted — every later poll repeats the drop, so the unlocked
// A68 starves while the head stays locked.
func TestTrimFairCarryDropsViableTail(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Millisecond)}
	}
	var carry []backend.FairTaskRef
	for id := int64(2); id <= 68; id++ {
		carry = append(carry, ref(id, "A"))
	}
	secured := []backend.FairTaskRef{ref(1000, "B")}
	trimmed := backend.TrimFairCarry(carry, secured, 2, 1)
	if len(trimmed) != 1+backend.FairCarryMargin {
		t.Fatalf("trimmed carry = %d rows, want 1 quota + %d margin", len(trimmed), backend.FairCarryMargin)
	}
	if trimmed[len(trimmed)-1].ID != 66 {
		t.Fatalf("trimmed tail = A%d, want A66 (A67,A68 dropped past the margin)", trimmed[len(trimmed)-1].ID)
	}
}

// TestTrimFairCarryWithResumeSpillsResumeCursor covers the round-19 P1 fix
// for the scenario above: the same trim must spill a resume cursor for the
// dropped tail, with the FIFO-next dropped row riding along (the overflow
// keyset requery is exclusive, so resuming exactly at the dropped minimum
// would skip it).
func TestTrimFairCarryWithResumeSpillsResumeCursor(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Millisecond)}
	}
	var carry []backend.FairTaskRef
	for id := int64(2); id <= 68; id++ {
		carry = append(carry, ref(id, "A"))
	}
	secured := []backend.FairTaskRef{ref(1000, "B")}
	kept, resume, dropped := backend.TrimFairCarryWithResume(carry, secured, 2, 1)
	if !dropped {
		t.Fatal("dropped = false, want true (A68 remains past quota+margin+boundary)")
	}
	// Quota (A2) + margin (A3..A66) + boundary (A67).
	if len(kept) != 1+backend.FairCarryMargin+1 {
		t.Fatalf("kept = %d rows, want 1 quota + %d margin + 1 boundary", len(kept), backend.FairCarryMargin)
	}
	if kept[len(kept)-1].ID != 67 {
		t.Fatalf("kept tail = A%d, want boundary A67", kept[len(kept)-1].ID)
	}
	if resume.ID != 68 || resume.InstanceID != "A" {
		t.Fatalf("resume = %v, want A68", resume)
	}
	// The requery from the cursor (exclusive) revisits exactly the dropped
	// remainder: everything at or before the boundary is retained.
	for _, r := range kept {
		if !backend.FairRefBefore(r, resume) {
			t.Fatalf("kept row A%d sorts at/after resume A68", r.ID)
		}
	}
}

// TestTrimFairCarryWithResumeNoSpillWhenFits covers the common case: when
// the whole carry fits in quota+margin nothing is dropped, no cursor spills,
// and kept matches the plain trim exactly.
func TestTrimFairCarryWithResumeNoSpillWhenFits(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Millisecond)}
	}
	pending := []backend.FairTaskRef{ref(2, "A"), ref(3, "A")}
	secured := []backend.FairTaskRef{ref(4, "B")}
	kept, resume, dropped := backend.TrimFairCarryWithResume(pending, secured, 2, 1)
	if dropped {
		t.Fatalf("dropped = true with resume %v, want no spill when the carry fits", resume)
	}
	plain := backend.TrimFairCarry(pending, secured, 2, 1)
	if len(kept) != len(plain) {
		t.Fatalf("kept = %v, want plain trim %v", kept, plain)
	}
	for i := range kept {
		if kept[i] != plain[i] {
			t.Fatalf("kept = %v, want plain trim %v", kept, plain)
		}
	}
}

// TestTrimFairCarryWithResumeSingleDrop covers the boundary case: exactly
// one row past the window rides along in kept with nothing left to revisit,
// so no cursor spills (no requery needed).
func TestTrimFairCarryWithResumeSingleDrop(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Millisecond)}
	}
	var carry []backend.FairTaskRef
	for id := int64(2); id <= 2+backend.FairCarryMargin+1; id++ {
		carry = append(carry, ref(id, "A"))
	}
	secured := []backend.FairTaskRef{ref(1000, "B")}
	kept, resume, dropped := backend.TrimFairCarryWithResume(carry, secured, 2, 1)
	if dropped {
		t.Fatalf("dropped = true with resume %v, want false (single excess row rides along)", resume)
	}
	if len(kept) != len(carry) {
		t.Fatalf("kept = %d rows, want whole carry %d (boundary absorbs the single drop)", len(kept), len(carry))
	}
}

// TestFairRefBeforeOrdersScanKeys pins the cursor comparison the claim loops
// use to keep the earliest overflow snapshot: (VisibleAt, ID) order,
// matching SortFairRefs.
func TestFairRefBeforeOrdersScanKeys(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := backend.FairTaskRef{ID: 1, InstanceID: "A", VisibleAt: base}
	b := backend.FairTaskRef{ID: 2, InstanceID: "A", VisibleAt: base}
	c := backend.FairTaskRef{ID: 1, InstanceID: "A", VisibleAt: base.Add(time.Millisecond)}
	if !backend.FairRefBefore(a, b) || backend.FairRefBefore(b, a) {
		t.Fatal("ID tie-break wrong")
	}
	if !backend.FairRefBefore(b, c) || backend.FairRefBefore(c, b) {
		t.Fatal("VisibleAt ordering wrong")
	}
	if backend.FairRefBefore(a, a) {
		t.Fatal("reflexive Before must be false")
	}
}
