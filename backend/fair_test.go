package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestFairRefsSortRestoresFIFO covers the issue #294 round-12 P2 at the
// helper level: refill passes secure later rows before earlier ones (pass 1
// locks B1 while the FIFO-earlier A2 is only secured on a refill after the
// pick that blocked it is lost), so the secured batch must be re-sorted by
// the refs' captured (visible_at, id) scan keys before it is returned — with
// no side rank map retaining every scanned candidate.
func TestFairRefsSortRestoresFIFO(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Scan order: A1, A2, B1 (FIFO); keys captured at scan time.
	a2 := backend.FairTaskRef{ID: 12, InstanceID: "A", VisibleAt: base.Add(time.Second)}
	b1 := backend.FairTaskRef{ID: 21, InstanceID: "B", VisibleAt: base.Add(2 * time.Second)}

	// Lock order from the refill scenario: B1 secured on pass 1, A2 on the
	// refill after A1 was lost to a concurrent lock.
	refs := []backend.FairTaskRef{b1, a2}
	backend.SortFairRefs(refs)
	if len(refs) != 2 || refs[0].ID != 12 || refs[1].ID != 21 {
		t.Fatalf("sorted = %v, want [{12 A} {21 B}]", refs)
	}

	// Equal visible_at orders by id, matching ORDER BY visible_at, id.
	ties := []backend.FairTaskRef{
		{ID: 7, InstanceID: "B", VisibleAt: base},
		{ID: 5, InstanceID: "A", VisibleAt: base},
	}
	backend.SortFairRefs(ties)
	if ties[0].ID != 5 || ties[1].ID != 7 {
		t.Fatalf("tie sort = %v, want IDs [5 7]", ties)
	}

	// Scan keys win over IDs: a later-created row (bigger ID) with an
	// earlier scan key still sorts first. Sorting by ID alone would emit
	// [B1 A2] here and break FIFO.
	opposed := []backend.FairTaskRef{
		{ID: 5, InstanceID: "B", VisibleAt: base.Add(2 * time.Second)},
		{ID: 12, InstanceID: "A", VisibleAt: base.Add(time.Second)},
	}
	backend.SortFairRefs(opposed)
	if opposed[0].ID != 12 || opposed[1].ID != 5 {
		t.Fatalf("opposed sort = %v, want [{12 A} {5 B}] (keys, not IDs)", opposed)
	}

	// Already-ordered input stays put.
	ordered := []backend.FairTaskRef{a2, b1}
	backend.SortFairRefs(ordered)
	if ordered[0].ID != 12 || ordered[1].ID != 21 {
		t.Fatalf("ordered sort = %v, want [{12 A} {21 B}]", ordered)
	}
}
