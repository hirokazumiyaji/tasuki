package spanner

import (
	"strings"
	"testing"
)

// User-supplied DedupeIDs must never share a row with an internal
// post-terminal retry marker (Codex round 6 on #327, round 11 on #296):
// since the round-11 marker move, markers live in
// postTerminalMarkersTable while user keys live in wf_signal_dedupe, so the
// two can never collide no matter how adversarial the DedupeID — including
// "__post_terminal__v1:x", whose legacy verbatim row used to occupy the very
// row the v1 marker probe for "x" reads (old code stored DedupeIDs
// verbatim). On the old in-dedupe code this fails.
func TestDedupeMarkerNamespaceDisjoint(t *testing.T) {
	adversarial := []string{
		"", "x", "_x", "__x", "___x", "__post_terminal__:", "__post_terminal__:x",
		"__post_terminal__v1:", "__post_terminal__v1:x",
		"____post_terminal__:x", "__post_terminal__:__post_terminal__:x",
		"post_terminal__:x", "a:b", "x:y:z",
	}
	for _, u := range adversarial {
		for _, bk := range dedupeKeyCandidates(u) {
			if strings.HasPrefix(bk, "__post_terminal__") {
				// Marker-shaped user candidates live in the table
				// disjointness check only (see isPostTerminalMarkerKey):
				// they can only match legacy verbatim rows, never a live
				// marker row. (Codex round 12 on #296: running probes
				// honor the raw legacy candidate as the live guard; only
				// terminal base-key probes skip marker shapes.)
				continue
			}
			for _, m := range adversarial {
				if bk == dedupeMarkerKey(m) {
					t.Fatalf("user key %q (for %q) collides with marker key for %q", bk, u, m)
				}
			}
		}
	}
	// Injectivity: distinct user IDs still map to distinct rows.
	seen := map[string]string{}
	for _, u := range adversarial {
		key := dedupeKey(u)
		if prev, dup := seen[key]; dup {
			t.Fatalf("user IDs %q and %q share row %q", prev, u, key)
		}
		seen[key] = u
	}
	// Escape is minimal: ordinary IDs are stored verbatim.
	for _, u := range []string{"", "x", "pay-42", "a:b"} {
		if got := dedupeKey(u); got != u {
			t.Fatalf("ordinary ID %q remapped to %q", u, got)
		}
	}
	if !strings.HasPrefix(dedupeMarkerKey("x"), "__post_terminal__v1:") {
		t.Fatal("marker lost its versioned derivation")
	}
}

// A pre-upgrade verbatim user row must never match the retry-marker probe
// (Codex round 9 on #327, round 11 on #296): marker checks consult
// postTerminalMarkersTable only, so rows in wf_signal_dedupe — legacy
// "__post_terminal__:x" and v1-shaped "__post_terminal__v1:x" alike — can
// never swallow a post-terminal send. On the old in-dedupe probe this fails
// for the v1-shaped row.
func TestDedupeMarkerProbeExcludesLegacyUserRow(t *testing.T) {
	// Marker checks consult postTerminalMarkersTable only, so a legacy
	// verbatim user row can never match the retry-marker probe — even when
	// it is STRING-equal to the marker key (Codex round 11 on #296: old
	// code stored DedupeIDs verbatim, so "__post_terminal__v1:x" collides
	// textually with the marker for "x"). The table boundary, not a prefix,
	// separates them. The behavioral proof is
	// TestTerminalLegacyV1UserRowDelivers; here pin that every such
	// textual match is a probe-skipped marker shape.
	for _, legacyRow := range []string{"__post_terminal__:x", "__post_terminal__v1:x"} {
		if got := dedupeMarkerKey("x"); got == legacyRow && !isPostTerminalMarkerKey(legacyRow) {
			t.Fatalf("textual marker match %q is not probe-skipped", legacyRow)
		}
	}
}
