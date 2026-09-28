package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestTrimFairCarryKeepsLockSkipFallbacks covers issue #294: a
// quota-only trim discards the rows a lock-skip round needs as replacements.
// Limit=2/MaxPerInstance=1 over A1,A2,A3,B1 with A1+A2 locked: pass 1 secures
// B1 and carries A2,A3. The trim must keep A2 (quota) AND A3 (fallback), so
// that losing A2 to SKIP LOCKED still leaves A3 for the next pass instead of
// an underfilled batch.
func TestTrimFairCarryKeepsLockSkipFallbacks(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string, ms int) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(ms) * time.Millisecond)}
	}
	pending := []backend.FairTaskRef{ref(2, "A", 2), ref(3, "A", 3)}
	secured := []backend.FairTaskRef{ref(4, "B", 4)}
	trimmed := backend.TrimFairCarry(pending, secured, 2, 1)
	if len(trimmed) != 2 || trimmed[0].ID != 2 || trimmed[1].ID != 3 {
		t.Fatalf("trimmed carry = %v, want [A2 A3] (quota + fallback)", trimmed)
	}

	// Simulate the lock-skip round: the picker (limit = remaining = 1)
	// admits A2, the lock step loses it, and the unoffered margin tail must
	// survive for the next pass.
	picker := backend.NewFairPicker(1, 1)
	picker.Seed(secured)
	offered := 0
	for _, r := range trimmed {
		if picker.Full() {
			break
		}
		picker.Offer(r)
		offered++
	}
	if len(picker.Picked()) != 1 || picker.Picked()[0].ID != 2 {
		t.Fatalf("picked = %v, want [A2]", picker.Picked())
	}
	tail := trimmed[offered:]
	if len(tail) != 1 || tail[0].ID != 3 {
		t.Fatalf("unoffered tail = %v, want [A3] for the refill pass", tail)
	}
	// The refill pass re-trims the tail and must still admit A3.
	retrimmed := backend.TrimFairCarry(tail, secured, 2, 1)
	if len(retrimmed) != 1 || retrimmed[0].ID != 3 {
		t.Fatalf("retrimmed tail = %v, want [A3]", retrimmed)
	}
}

// TestTrimFairCarryMarginBounded checks that the retained set stays
// O(limit+margin), never O(carry),
// and rows skipped as over-quota before the quota fill stay dropped (no
// instance at the cap can admit them later in the claim).
func TestTrimFairCarryMarginBounded(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var flood []backend.FairTaskRef
	for i := 0; i < 2000; i++ {
		flood = append(flood, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A", VisibleAt: base.Add(time.Duration(i) * time.Millisecond)})
	}
	trimmed := backend.TrimFairCarry(flood, nil, 2, 1)
	// Quota fill never completes (single instance, per-instance 1,
	// remaining 2), so the margin opens right after the one quota row:
	// still O(limit+margin), never O(carry).
	if len(trimmed) != 2+backend.FairCarryMargin {
		t.Fatalf("trimmed flood = %d rows, want remaining(2) + %d margin", len(trimmed), backend.FairCarryMargin)
	}
	// Forcing an early quota fill (limit 1) opens exactly one margin window.
	trimmed = backend.TrimFairCarry(flood, nil, 1, 1)
	if len(trimmed) != 1+backend.FairCarryMargin {
		t.Fatalf("trimmed flood = %d rows, want 1 quota + %d margin", len(trimmed), backend.FairCarryMargin)
	}
	for i, r := range trimmed {
		if r.ID != int64(i+1) {
			t.Fatalf("trimmed[%d] = %d, want FIFO %d", i, r.ID, i+1)
		}
	}
}
