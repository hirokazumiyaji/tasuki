package spanner

import (
	"strings"
	"testing"
)

// User-supplied DedupeIDs must never share a row with an internal
// post-terminal retry marker (Codex round 6 on #327): without namespacing, a
// pre-terminal send with DedupeID "__post_terminal__:x" occupied the very
// row the marker check for user ID "x" reads, so the first post-terminal
// send of "x" was swallowed as a retry (lost signal).
func TestDedupeMarkerNamespaceDisjoint(t *testing.T) {
	adversarial := []string{
		"", "x", "_x", "__x", "___x", "__post_terminal__:", "__post_terminal__:x",
		"__post_terminal__v1:", "__post_terminal__v1:x",
		"____post_terminal__:x", "__post_terminal__:__post_terminal__:x",
		"post_terminal__:x", "a:b", "x:y:z",
	}
	users := map[string]string{}
	for _, u := range adversarial {
		users[u] = dedupeKey(u)
	}
	for _, m := range adversarial {
		marker := dedupeMarkerKey(m)
		for u, key := range users {
			if key == marker {
				t.Fatalf("user key %q (for %q) collides with marker for %q", key, u, m)
			}
		}
	}
	// Injectivity: distinct user IDs still map to distinct rows.
	seen := map[string]string{}
	for _, u := range adversarial {
		if prev, dup := seen[users[u]]; dup {
			t.Fatalf("user IDs %q and %q share row %q", prev, u, users[u])
		}
		seen[users[u]] = u
	}
	// Escape is minimal: ordinary IDs are stored verbatim.
	for _, u := range []string{"", "x", "pay-42", "a:b"} {
		if got := dedupeKey(u); got != u {
			t.Fatalf("ordinary ID %q remapped to %q", u, got)
		}
	}
	if !strings.HasPrefix(dedupeMarkerKey("x"), "__post_terminal__v1:") {
		t.Fatal("marker lost its versioned prefix")
	}
}

// A pre-upgrade verbatim user row ("__post_terminal__:x", stored before the
// round-6 escape) must never match the marker probe for "x" (Codex round 9
// on #327): the dual-read mistook it for a retry marker and dropped the
// first post-terminal send of "x". Versioned probes exclude legacy forms by
// construction; on the old code this fails.
func TestDedupeMarkerProbeExcludesLegacyUserRow(t *testing.T) {
	const legacyRow = "__post_terminal__:x"
	for _, mk := range dedupeMarkerCandidates("x") {
		if mk == legacyRow {
			t.Fatalf("marker probe for %q matches legacy user row %q", "x", legacyRow)
		}
	}
	if got := dedupeMarkerKey("x"); got == legacyRow {
		t.Fatalf("versioned marker %q collides with legacy user row", got)
	}
}
