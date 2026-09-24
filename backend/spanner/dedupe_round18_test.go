package spanner

import (
	"testing"
)

// TestPickSpannerDedupeInsert covers the round-18 P2 batch reservation: keys
// chosen earlier in the batch count as occupied, so two items never emit
// colliding inserts for one key (transaction reads don't see buffered
// mutations — the whole batch would otherwise fail deterministically on
// every retry).
func TestPickSpannerDedupeInsert(t *testing.T) {
	const canon, fallback = "______x", "____x"
	t.Run("free canonical", func(t *testing.T) {
		key, ver, ok := pickSpannerDedupeInsert(canon, fallback, false, false, map[string]bool{})
		if !ok || key != canon || ver != int64(dedupeFormatVersion) {
			t.Fatalf("got %q %d %v, want canonical v1", key, ver, ok)
		}
	})
	t.Run("occupied canonical falls back", func(t *testing.T) {
		key, ver, ok := pickSpannerDedupeInsert(canon, fallback, true, false, map[string]bool{})
		if !ok || key != fallback || ver != int64(dedupeFormatRawKeyVersion) {
			t.Fatalf("got %q %d %v, want fallback v2", key, ver, ok)
		}
	})
	t.Run("reserved canonical moves to fallback", func(t *testing.T) {
		reserved := map[string]bool{canon: true}
		key, _, ok := pickSpannerDedupeInsert(canon, fallback, false, false, reserved)
		if !ok || key != fallback {
			t.Fatalf("got %q %v, want fallback (canonical reserved)", key, ok)
		}
	})
	t.Run("batch collision assigns distinct keys", func(t *testing.T) {
		// Batch "____x" (canonical "______x" foreign-occupied) + "__x"
		// (canonical "____x"): the first takes fallback "____x"; the
		// second must not reuse it.
		reserved := map[string]bool{}
		k1, _, ok := pickSpannerDedupeInsert("______x", "____x", true, false, reserved)
		if !ok || k1 != "____x" {
			t.Fatalf("item 1 got %q %v, want fallback ____x", k1, ok)
		}
		reserved[k1] = true
		k2, _, ok := pickSpannerDedupeInsert("____x", "__x", false, false, reserved)
		if !ok || k2 != "__x" {
			t.Fatalf("item 2 got %q %v, want its own fallback __x", k2, ok)
		}
	})
	t.Run("nothing free inserts unguarded", func(t *testing.T) {
		if _, _, ok := pickSpannerDedupeInsert(canon, fallback, true, true, map[string]bool{}); ok {
			t.Fatal("fully occupied keys must yield no insert (unguarded)")
		}
		reserved := map[string]bool{canon: true, fallback: true}
		if _, _, ok := pickSpannerDedupeInsert(canon, fallback, false, false, reserved); ok {
			t.Fatal("fully reserved keys must yield no insert (unguarded)")
		}
	})
}
