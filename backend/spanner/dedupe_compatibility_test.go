package spanner

import (
	"strings"
	"testing"
)

func TestDedupeEncodingCompatibility(t *testing.T) {
	long := strings.Repeat("k", 300)
	vectors := []struct {
		raw        string
		key        string
		marker     string
		fallback   string
		candidates []string
	}{
		{raw: "x", key: "x", marker: "__post_terminal__v1:x", fallback: "x", candidates: []string{"x"}},
		{raw: "__x", key: "____x", marker: "__post_terminal__v1:__x", fallback: "__x", candidates: []string{"__x", "____x"}},
		{
			raw:        long,
			key:        "__hash__:17b16d8ef494060fefa36a6a41567b8c32d213a17e02b7eeae86158cc4495461",
			marker:     "__post_terminal__v1#h:17b16d8ef494060fefa36a6a41567b8c32d213a17e02b7eeae86158cc4495461",
			fallback:   "__hash__:raw:492109eb1c737415b4ea39e82cb5944fc55679c2ec63645e4a3a029859a1d6b1",
			candidates: []string{long, "__hash__:17b16d8ef494060fefa36a6a41567b8c32d213a17e02b7eeae86158cc4495461", "__hash__:raw:492109eb1c737415b4ea39e82cb5944fc55679c2ec63645e4a3a029859a1d6b1"},
		},
	}
	for _, vector := range vectors {
		if got := escapeDedupeID(vector.raw); got != vector.key {
			t.Errorf("escapeDedupeID(%q) = %q, want %q", vector.raw, got, vector.key)
		}
		if got := postTerminalDedupeMarker(vector.raw); got != vector.marker {
			t.Errorf("postTerminalDedupeMarker(%q) = %q, want %q", vector.raw, got, vector.marker)
		}
		if got := rawFallbackDedupeKey(vector.raw); got != vector.fallback {
			t.Errorf("rawFallbackDedupeKey(%q) = %q, want %q", vector.raw, got, vector.fallback)
		}
		got := dedupeKeyCandidates(vector.raw)
		if len(got) != len(vector.candidates) {
			t.Errorf("dedupeKeyCandidates(%q) = %q, want %q", vector.raw, got, vector.candidates)
			continue
		}
		for i := range got {
			if got[i] != vector.candidates[i] {
				t.Errorf("dedupeKeyCandidates(%q) = %q, want %q", vector.raw, got, vector.candidates)
				break
			}
		}
	}
}
