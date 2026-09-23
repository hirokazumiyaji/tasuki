package spanner

import (
	"strings"
	"testing"
)

// Long DedupeIDs must stay within the wf_signal_dedupe.dedupe_id STRING(255)
// budget (Codex round 8 on #327): the old verbatim escape/marker
// concatenation exceeded it for 238-255-char IDs (marker) and 254-255-char
// "__" IDs (escape adds 2). On the old code len() here exceeds the budget
// and this fails.
func TestDedupeKeysBoundedByColumnLimit(t *testing.T) {
	lengths := []int{0, 1, 200, 236, 237, 238, 239, 253, 254, 255, 256, 300, 1000}
	for _, n := range lengths {
		for _, raw := range []string{strings.Repeat("k", n), "__" + strings.Repeat("k", n)} {
			if got := escapeDedupeID(raw); len(got) > dedupeKeyLimit {
				t.Fatalf("escapeDedupeID(%d chars, __=%v) = %d bytes, want <= %d", len(raw), strings.HasPrefix(raw, "__"), len(got), dedupeKeyLimit)
			}
			if got := postTerminalDedupeMarker(raw); len(got) > dedupeKeyLimit {
				t.Fatalf("postTerminalDedupeMarker(%d chars) = %d bytes, want <= %d", len(raw), len(got), dedupeKeyLimit)
			}
		}
	}
	// Short IDs keep the exact historical user encoding; markers carry the
	// round-9 namespace version: hashing only kicks in over budget.
	for _, raw := range []string{"", "x", "pay-42", "a:b"} {
		if got := escapeDedupeID(raw); got != raw {
			t.Fatalf("short ID %q remapped to %q", raw, got)
		}
		if got := postTerminalDedupeMarker(raw); got != "__post_terminal__v1:"+raw {
			t.Fatalf("short marker for %q = %q", raw, got)
		}
	}
	if got := escapeDedupeID("__x"); got != "____x" {
		t.Fatalf("short __ ID escaped to %q, want ____x", got)
	}
}

// Hashed forms must stay disjoint from every other namespace, however
// adversarial the raw ID (Codex round 8 on #327).
func TestDedupeHashedNamespaceDisjoint(t *testing.T) {
	longA := strings.Repeat("a", 300)
	longB := strings.Repeat("b", 300)
	longUnder := "__" + strings.Repeat("u", 300)
	users := map[string]string{
		"x":         dedupeKey("x"),
		"__x":       dedupeKey("__x"),
		"longA":     dedupeKey(longA),
		"longB":     dedupeKey(longB),
		"longUnder": dedupeKey(longUnder),
		"hashlike":  dedupeKey("__hash__:abc"),
	}
	markers := map[string]string{
		"x":     dedupeMarkerKey("x"),
		"longA": dedupeMarkerKey(longA),
		"longB": dedupeMarkerKey(longB),
	}
	for uk, u := range users {
		for mk, m := range markers {
			if u == m {
				t.Fatalf("user key %q (%q) collides with marker %q (%q)", u, uk, m, mk)
			}
		}
	}
	if users["longA"] == users["longB"] {
		t.Fatal("distinct long IDs hashed to the same user key")
	}
	if markers["longA"] == markers["longB"] {
		t.Fatal("distinct long IDs hashed to the same marker")
	}
	adversarial := users["longA"]
	if got := dedupeKey(adversarial); got == adversarial {
		t.Fatalf("hash-like raw %q stored verbatim, colliding with a hashed row", adversarial)
	}
}

// Dedupe-check reads must probe the legacy raw form first (Codex round 8 on
// #327): pre-escape rows stored "__" IDs verbatim, so probing only the
// escaped form misses and duplicates the event.
func TestDedupeKeyCandidatesLegacyFirst(t *testing.T) {
	if got := dedupeKeyCandidates("x"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("candidates(x) = %q, want [x]", got)
	}
	got := dedupeKeyCandidates("__x")
	if len(got) != 2 || got[0] != "__x" || got[1] != "____x" {
		t.Fatalf("candidates(__x) = %q, want [__x ____x] (raw first)", got)
	}
	long := "__" + strings.Repeat("k", 300)
	lg := dedupeKeyCandidates(long)
	if len(lg) < 3 || lg[0] != long {
		t.Fatalf("long candidates must start with legacy raw, got %q", lg)
	}
	if lg[len(lg)-1] != dedupeKey(long) {
		t.Fatalf("long candidates must end with current encoding, got %q", lg)
	}
}

func TestDedupeMarkerCandidatesForms(t *testing.T) {
	// Marker probes check the versioned form only (Codex round 9 on #327):
	// legacy unversioned rows must never match, so a pre-upgrade verbatim
	// user key "__post_terminal__:x" cannot swallow the post-terminal send
	// of "x". On the old dual-read code the legacy form was probed and this
	// fails.
	if got := dedupeMarkerCandidates("x"); len(got) != 1 || got[0] != "__post_terminal__v1:x" {
		t.Fatalf("marker candidates(x) = %q", got)
	}
	for _, mk := range dedupeMarkerCandidates("x") {
		if mk == "__post_terminal__:x" {
			t.Fatalf("versioned marker probe matches legacy user row %q", mk)
		}
	}
	long := strings.Repeat("k", 300)
	got := dedupeMarkerCandidates(long)
	if len(got) != 1 || got[0] != dedupeMarkerKey(long) {
		t.Fatalf("long marker candidates = %q, want [versioned-hashed]", got)
	}
}

func TestIsPostTerminalMarkerKey(t *testing.T) {
	for _, k := range []string{
		"__post_terminal__:x",
		"__post_terminal__:" + strings.Repeat("k", 300),
		dedupeMarkerKey("x"),
		dedupeMarkerKey(strings.Repeat("k", 300)),
	} {
		if !isPostTerminalMarkerKey(k) {
			t.Fatalf("marker key %q not detected", k)
		}
	}
	for _, k := range []string{"x", "____x", "__hash__:abc", dedupeKey(strings.Repeat("k", 300))} {
		if isPostTerminalMarkerKey(k) {
			t.Fatalf("user key %q misdetected as marker", k)
		}
	}
}

// The terminate sweep must preserve markers while reaping pre-termination
// base keys (Codex round 8 on #327). On the old unqualified sweep every key
// was deleted and this fails.
func TestFilterTerminateDedupePreservesMarkers(t *testing.T) {
	base := dedupeKey("k1")
	marker := dedupeMarkerKey("k2")
	hashedMarker := dedupeMarkerKey(strings.Repeat("k", 300))
	snapshot := []string{base, marker, hashedMarker}
	got := filterTerminateDedupeKeys(snapshot)
	if len(got) != 1 || got[0] != base {
		t.Fatalf("filtered sweep = %q, want only the base key", got)
	}
}
