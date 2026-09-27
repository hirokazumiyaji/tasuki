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
	wantLong := []string{long, "__" + long, dedupeKey(long), rawFallbackDedupeKey(long)}
	if len(lg) != len(wantLong) {
		t.Fatalf("long candidates = %q, want %q", lg, wantLong)
	}
	for i := range wantLong {
		if lg[i] != wantLong[i] {
			t.Fatalf("long candidates = %q, want %q", lg, wantLong)
		}
	}
}

func TestPostTerminalMarkerDocSeparation(t *testing.T) {
	// Since the round-11 marker move, retry markers live in
	// postTerminalMarkersTable, never in wf_signal_dedupe — so a legacy
	// verbatim user row may be STRING-equal to a marker key (e.g.
	// "__post_terminal__v1:x" for the marker of "x") without colliding:
	// user-key probes skip marker-shaped candidates and marker probes never
	// consult wf_signal_dedupe. Pin the operative invariant: every user
	// candidate that string-matches a marker key is marker-shaped, hence
	// skipped by every probe. On the old in-dedupe code the raw candidate
	// was consulted and this fails.
	adversarial := []string{"", "x", "__post_terminal__:x", "__post_terminal__v1:x", "__x"}
	for _, u := range adversarial {
		for _, bk := range dedupeKeyCandidates(u) {
			for _, m := range adversarial {
				if bk == dedupeMarkerKey(m) && !isPostTerminalMarkerKey(bk) {
					t.Fatalf("user candidate %q (for %q) matches marker key for %q yet is not probe-skipped", bk, u, m)
				}
			}
		}
	}
	long := strings.Repeat("k", 300)
	if got := dedupeMarkerKey(long); got == "" {
		t.Fatal("long marker key must be non-empty (bounded hashed form)")
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
	for _, k := range []string{"x", "____x", "__hash__:abc", dedupeKey(strings.Repeat("k", 300)), rawFallbackDedupeKey(strings.Repeat("k", 300))} {
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
