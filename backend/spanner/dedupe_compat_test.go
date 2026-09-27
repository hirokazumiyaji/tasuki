package spanner

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
)

// TestSoleAmbiguousGuardKey covers the round-28 P1 placement on #296: for
// an ambiguously-encoded DedupeID (canonical form differs from the raw ID)
// the guard lives solely at the fallback key — never at the canonical key,
// where a pre-upgrade node probing verbatim would mistake it for its own
// guard and silently drop that ID's first send.
func TestSoleAmbiguousGuardKey(t *testing.T) {
	// Short ambiguous ID: sole guard at the raw key itself, legacy
	// verbatim shape (version 0 = no version/owner columns).
	if key, ver, ok := soleAmbiguousGuardKey("__x", "__x", false, map[string]bool{}); !ok || key != "__x" || ver != 0 {
		t.Fatalf("sole(__x) = %q %d %v, want __x 0 true", key, ver, ok)
	}
	// Over-budget ambiguous ID: sole guard at the bounded fallback with v2
	// (the caller records the owner's canonical form alongside).
	long := strings.Repeat("k", 300)
	fb := rawFallbackDedupeKey(long)
	if fb == long || escapeDedupeID(long) == long {
		t.Fatal("test setup: long ID must be ambiguously encoded")
	}
	if key, ver, ok := soleAmbiguousGuardKey(long, fb, false, map[string]bool{}); !ok || key != fb || ver != int64(dedupeFormatRawKeyVersion) {
		t.Fatalf("sole(long) = %q %d %v, want %q v2 true", key, ver, ok, fb)
	}
	// Occupied fallback: no guard (caller inserts unguarded,
	// duplicate-never-drop) rather than a canonical row old nodes misread.
	if _, _, ok := soleAmbiguousGuardKey("__x", "__x", true, map[string]bool{}); ok {
		t.Fatal("sole with occupied fallback must be false (unguarded insert)")
	}
	// Batch-reserved fallback: same.
	if _, _, ok := soleAmbiguousGuardKey("__x", "__x", false, map[string]bool{"__x": true}); ok {
		t.Fatal("sole with reserved fallback must be false (unguarded insert)")
	}
}

// TestSoleGuardMatchesAndIsolates pins the read side of the round-28 P1
// placement: the sole-guard shapes resolve for current readers, an old node
// probing verbatim hits exactly its own ID's guard, and the "__x"/"____x"
// pair never claims each other's rows in either direction.
func TestSoleGuardMatchesAndIsolates(t *testing.T) {
	const x = "__x"
	const s = "____x" // escapeDedupeID(x): a DISTINCT user ID
	if escapeDedupeID(x) != s {
		t.Fatal("test setup: escape(__x) must be ____x")
	}
	// Sole verbatim guard at "__x": X matches (legacy exact-raw rule —
	// readable by old nodes via existence and by current readers here).
	if !matchDedupeRow(x, x, x, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("X misses its own sole guard")
	}
	// S must not claim X's guard (the round-13/19 ownership rule that
	// current readers already enforce).
	if matchDedupeRow(s, x, x, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("S claims X's sole guard (would drop S)")
	}
	// And X must not claim S's verbatim guard either (coexistence when an
	// old node wrote S first, or a new node sends S later).
	if matchDedupeRow(x, s, s, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("X claims S's guard (would drop X)")
	}
	// Over-budget sole guard: v2 fallback row with owner matches only its
	// owner (the pre-existing v2 rule, unchanged).
	long := strings.Repeat("k", 300)
	fb := rawFallbackDedupeKey(long)
	owner := spanner.NullString{StringVal: escapeDedupeID(long), Valid: true}
	version := spanner.NullInt64{Int64: int64(dedupeFormatRawKeyVersion), Valid: true}
	if !matchDedupeRow(long, fb, fb, owner, version) {
		t.Fatal("long ID misses its own v2 sole guard")
	}
	if matchDedupeRow(fb, fb, fb, owner, version) {
		t.Fatal("literal fallback-hash ID claims the long ID's guard")
	}
}

// TestCompatLegMatchesOldReader pins the legacy-rule matching the round-28
// sole guard relies on: the verbatim "__x" row (no version columns) matches
// an old node probing ONLY the raw verbatim key by existence as well as a
// current reader by exact raw equality, while the distinct ID "____x"
// matches neither it nor, by symmetry, its own verbatim row for "__x".
// (Supersedes the round-26 escaped-primary-plus-leg shape: the sole guard IS
// this legacy-shaped row, with no canonical row beside it.)
func TestCompatLegMatchesOldReader(t *testing.T) {
	const raw = "__x"
	primary := escapeDedupeID(raw) // "____x"
	if primary == raw {
		t.Fatal("test setup: __x must escape")
	}
	// Legacy-shaped sole-guard row: stored raw, no version.
	if !matchDedupeRow(raw, raw, raw, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("old reader probing the raw sole guard misses it")
	}
	// A different ID must not claim the sole guard.
	if matchDedupeRow("____x", raw, raw, spanner.NullString{}, spanner.NullInt64{}) {
		t.Fatal("foreign ID claims the raw sole guard")
	}
}
