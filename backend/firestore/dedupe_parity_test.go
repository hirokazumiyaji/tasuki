package firestore

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/spanner"
)

// Both backends must encode DedupeIDs identically (Codex round-16 on #296):
// the key-level encodings are duplicated per backend (Firestore concatenates
// them into framed document IDs; Spanner uses composite keys), so pin the
// shared vectors in one place. Drift here means a DedupeID dedupes on one
// backend but not the other.
func TestDedupeEncodingMatchesSpanner(t *testing.T) {
	vectors := []string{
		"", "x", "pay-42", "a:b", "x:y:z",
		"__x", "___x", "__post_terminal__:x", "__post_terminal__v1:x",
		"__hash__:abc", "__hash__:raw:abc",
		strings.Repeat("k", 238), strings.Repeat("k", 255), strings.Repeat("k", 256),
		strings.Repeat("k", 300), strings.Repeat("k", 1000),
		"__" + strings.Repeat("u", 300),
	}
	for _, raw := range vectors {
		if got, want := escapeDedupeID(raw), spanner.EscapeDedupeID(raw); got != want {
			t.Fatalf("escapeDedupeID(%q) = %q, spanner = %q", raw, got, want)
		}
		if got, want := postTerminalDedupeMarker(raw), spanner.PostTerminalDedupeMarker(raw); got != want {
			t.Fatalf("postTerminalDedupeMarker(%q) = %q, spanner = %q", raw, got, want)
		}
		if got, want := rawFallbackDedupeKey(raw), spanner.RawFallbackDedupeKey(raw); got != want {
			t.Fatalf("rawFallbackDedupeKey(%q) = %q, spanner = %q", raw, got, want)
		}
		fc, sc := dedupeKeyCandidates(raw), spanner.DedupeKeyCandidates(raw)
		if len(fc) != len(sc) {
			t.Fatalf("dedupeKeyCandidates(%q) = %q, spanner = %q", raw, fc, sc)
		}
		for i := range fc {
			if fc[i] != sc[i] {
				t.Fatalf("dedupeKeyCandidates(%q) = %q, spanner = %q", raw, fc, sc)
			}
		}
	}
}
