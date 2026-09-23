package firestore

import (
	"strings"
	"testing"
)

// User-supplied DedupeIDs must never share a document with an internal
// post-terminal retry marker (Codex round 6 on #327, round 11 on #296):
// since the round-11 marker move, markers live in
// postTerminalMarkersCollection while user keys live in wf_signal_dedupe,
// so the two can never collide no matter how adversarial the DedupeID —
// including "__post_terminal__v1:x", whose legacy verbatim row used to
// occupy the very document the v1 marker probe for "x" reads (old code
// stored DedupeIDs verbatim). On the old in-dedupe code this fails.
func TestDedupeMarkerNamespaceDisjoint(t *testing.T) {
	const inst = "ns-test"
	adversarial := []string{
		"", "x", "_x", "__x", "___x", "__post_terminal__:", "__post_terminal__:x",
		"__post_terminal__v1:", "__post_terminal__v1:x",
		"____post_terminal__:x", "__post_terminal__:__post_terminal__:x",
		"post_terminal__:x", "a:b", "x:y:z",
	}
	for _, u := range adversarial {
		for _, bk := range dedupeKeyCandidates(u) {
			if strings.HasPrefix(bk, "__post_terminal__") {
				// Marker-shaped user candidates are skipped by every probe
				// (see isPostTerminalMarkerKey): they can only match inert
				// pre-upgrade rows, never the live guard.
				continue
			}
			for _, m := range adversarial {
				if userDoc, markerDoc := inst+":"+bk, postTerminalMarkerDocID(inst, m); userDoc == markerDoc {
					t.Fatalf("user doc %q (for %q) collides with marker doc for %q", userDoc, u, m)
				}
			}
		}
	}
	// Injectivity: distinct user IDs still map to distinct documents, so
	// normal (non-terminal) dedupe keeps working. Compared on current
	// (escaped) forms only: legacy raw probes intentionally overlap them
	// for pre-escape compat.
	seen := map[string]string{}
	for _, u := range adversarial {
		key := inst + ":" + escapeDedupeID(u)
		if prev, dup := seen[key]; dup {
			t.Fatalf("user IDs %q and %q share document %q", prev, u, key)
		}
		seen[key] = u
	}
	// Escape is minimal: ordinary IDs are stored verbatim.
	for _, u := range []string{"", "x", "pay-42", "a:b"} {
		if got := signalDedupeID(inst, u); got != inst+":"+u {
			t.Fatalf("ordinary ID %q remapped to %q", u, got)
		}
	}
	if !strings.HasPrefix(postTerminalDedupeMarker("x"), "__post_terminal__v1:") {
		t.Fatal("marker lost its versioned derivation")
	}
}

// A pre-upgrade verbatim user row must never match the retry-marker probe
// (Codex round 9 on #327, round 11 on #296): marker checks consult
// postTerminalMarkersCollection only, so rows in wf_signal_dedupe — legacy
// "__post_terminal__:x" and v1-shaped "__post_terminal__v1:x" alike — can
// never swallow a post-terminal send. On the old in-dedupe probe this fails
// for the v1-shaped row.
func TestDedupeMarkerProbeExcludesLegacyUserRow(t *testing.T) {
	// Marker checks consult postTerminalMarkersCollection only, so a legacy
	// verbatim user row can never match the retry-marker probe — even when
	// it is STRING-equal to the marker document suffix (Codex round 11 on
	// #296: old code stored DedupeIDs verbatim, so "__post_terminal__v1:x"
	// collides textually with the marker for "x"). The collection boundary,
	// not a prefix, separates them. The behavioral proof is
	// TestTerminalLegacyV1UserRowDelivers; here pin that every such
	// textual match is a probe-skipped marker shape.
	const inst = "ns-probe-test"
	for _, legacyRow := range []string{"__post_terminal__:x", "__post_terminal__v1:x"} {
		if got := postTerminalMarkerDocID(inst, "x"); got == inst+":"+legacyRow && !isPostTerminalMarkerKey(legacyRow) {
			t.Fatalf("textual marker match %q is not probe-skipped", legacyRow)
		}
	}
}
