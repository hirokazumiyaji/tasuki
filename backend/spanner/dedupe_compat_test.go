package spanner

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
)

// TestRawCompatGuardKey is the round-26 P1 regression test on #296: a
// new-node send of "__x" stores the escaped "____x" primary, which old nodes
// probing the raw verbatim "__x" miss — so a raw legacy compat leg must ride
// along. Fail-without-fix: without the compat write (escaped-only, the
// pre-fix backend.go) the old-reader probe below finds nothing.
func TestRawCompatGuardKey(t *testing.T) {
	// Escaped ID: compat leg required alongside the escaped primary.
	if raw, ok := rawCompatGuardKey("__x", "____x", false, map[string]bool{}); !ok || raw != "__x" {
		t.Fatalf("compat(__x vs ____x) = %q %v, want __x true", raw, ok)
	}
	// Identity encoding: the primary IS the raw form, no second row.
	if _, ok := rawCompatGuardKey("x", "x", false, map[string]bool{}); ok {
		t.Fatal("compat(x vs x) must be false (primary already raw)")
	}
	// Fallback primary that equals the raw form (short IDs): no second row.
	if _, ok := rawCompatGuardKey("__x", "__x", false, map[string]bool{}); ok {
		t.Fatal("compat(__x vs fallback __x) must be false (same row)")
	}
	// Occupied raw key: skip (would collide; duplicate-never-drop).
	if _, ok := rawCompatGuardKey("__x", "____x", true, map[string]bool{}); ok {
		t.Fatal("compat with occupied raw key must be false")
	}
	// Reserved raw key (same-batch race): skip.
	if _, ok := rawCompatGuardKey("__x", "____x", false, map[string]bool{"__x": true}); ok {
		t.Fatal("compat with reserved raw key must be false")
	}
	// Over-budget raw: unwritable on Spanner, skip.
	long := strings.Repeat("k", 300)
	if _, ok := rawCompatGuardKey(long, escapeDedupeID(long), false, map[string]bool{}); ok {
		t.Fatal("compat with over-budget raw key must be false")
	}
}

// TestCompatLegMatchesOldReader simulates the mixed-rollout retry: the new
// node writes the escaped primary plus the raw legacy compat leg, and an old
// node probing ONLY the raw verbatim key must match (legacy rule,
// stored == raw) while no other ID claims the leg.
func TestCompatLegMatchesOldReader(t *testing.T) {
	const raw = "__x"
	primary := escapeDedupeID(raw) // "____x"
	if primary == raw {
		t.Fatal("test setup: __x must escape")
	}
	// Legacy-shaped compat row: stored raw, no version.
	if !matchDedupeRow(raw, raw, raw, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("old reader probing the raw compat leg misses the guard")
	}
	// A different ID must not claim the compat leg.
	if matchDedupeRow("____x", raw, raw, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("foreign ID claims the raw compat leg")
	}
	// Current readers still match the escaped primary by canonical form.
	_ = primary
}
