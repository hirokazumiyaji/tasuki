package firestore

import (
	"strings"
	"testing"
)

// User-supplied DedupeIDs must never share a document with an internal
// post-terminal retry marker (Codex round 6 on #327): without namespacing, a
// pre-terminal send with DedupeID "__post_terminal__:x" occupied the very
// document the marker check for user ID "x" reads, so the first
// post-terminal send of "x" was swallowed as a retry (lost signal).
func TestDedupeMarkerNamespaceDisjoint(t *testing.T) {
	const inst = "ns-test"
	// The exact historical collision, expressed with pre-existing names
	// only: the stored user key for "__post_terminal__:x" must differ from
	// the marker document for "x" (instanceID + ":" + marker). On the old
	// code both were inst+":__post_terminal__:x" and this fails.
	if a, b := signalDedupeID(inst, "__post_terminal__:x"), inst+":"+postTerminalDedupeMarker("x"); a == b {
		t.Fatalf("user key and marker collide at %q", a)
	}
	adversarial := []string{
		"", "x", "_x", "__x", "___x", "__post_terminal__:", "__post_terminal__:x",
		"__post_terminal__v1:", "__post_terminal__v1:x",
		"____post_terminal__:x", "__post_terminal__:__post_terminal__:x",
		"post_terminal__:x", "a:b", "x:y:z",
	}
	users := map[string]string{}
	for _, u := range adversarial {
		users[u] = signalDedupeID(inst, u)
	}
	for _, m := range adversarial {
		marker := inst + ":" + postTerminalDedupeMarker(m)
		for u, key := range users {
			if key == marker {
				t.Fatalf("user key %q (for %q) collides with marker for %q", key, u, m)
			}
		}
	}
	// Injectivity: distinct user IDs still map to distinct documents, so
	// normal (non-terminal) dedupe keeps working.
	seen := map[string]string{}
	for _, u := range adversarial {
		if prev, dup := seen[users[u]]; dup {
			t.Fatalf("user IDs %q and %q share document %q", prev, u, users[u])
		}
		seen[users[u]] = u
	}
	// Escape is minimal: ordinary IDs are stored verbatim.
	for _, u := range []string{"", "x", "pay-42", "a:b"} {
		if got := signalDedupeID(inst, u); got != inst+":"+u {
			t.Fatalf("ordinary ID %q remapped to %q", u, got)
		}
	}
	if !strings.HasPrefix(postTerminalDedupeMarker("x"), "__post_terminal__v1:") {
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
	if got := postTerminalDedupeMarker("x"); got == legacyRow {
		t.Fatalf("versioned marker %q collides with legacy user row", got)
	}
}
