package backend_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestFairScanOrderRestoresFIFO covers the issue #294 round-11 P2 at the
// helper level: refill passes secure later rows before earlier ones (pass 1
// locks B1 while the FIFO-earlier A2 is only secured on a refill after the
// pick that blocked it is lost), so the secured batch must be re-sorted by
// first-seen scan position before it is returned.
func TestFairScanOrderRestoresFIFO(t *testing.T) {
	var order backend.FairScanOrder
	// Scan order: A1, A2, B1 (FIFO).
	for _, id := range []int64{11, 12, 21} {
		order.Note(id)
	}
	// Re-offers keep the earlier position.
	order.Note(12)

	// Lock order from the refill scenario: B1 secured on pass 1, A2 on the
	// refill after A1 was lost to a concurrent lock.
	refs := []backend.FairTaskRef{
		{ID: 21, InstanceID: "B"},
		{ID: 12, InstanceID: "A"},
	}
	order.SortRefs(refs)
	if len(refs) != 2 || refs[0].ID != 12 || refs[1].ID != 21 {
		t.Fatalf("sorted = %v, want [{12 A} {21 B}]", refs)
	}

	if got := order.Rank(11); got != 0 {
		t.Fatalf("Rank(A1) = %d, want 0 (first scanned)", got)
	}
	if got := order.Rank(21); got != 2 {
		t.Fatalf("Rank(B1) = %d, want 2 (last scanned)", got)
	}
	// Unnoted IDs sort last rather than first.
	if got := order.Rank(999); got <= order.Rank(21) {
		t.Fatalf("Rank(unnoted) = %d, want > Rank(B1)", got)
	}
}
