package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestTrimFairCarryBoundsLockedFlood covers issue #294 round-17 P2(a): a
// plain visibility probe cannot see locks, so probing the full retained carry
// retains every locked row and the picker admits one per pass (~2000 lock
// queries + quadratic re-offers). Trimming to the unfilled per-instance quota
// bounds the probe set to O(limit).
func TestTrimFairCarryBoundsLockedFlood(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var carry []backend.FairTaskRef
	for i := 0; i < 2000; i++ {
		carry = append(carry, backend.FairTaskRef{ID: int64(i + 1), InstanceID: "A", VisibleAt: base.Add(time.Duration(i) * time.Millisecond)})
	}
	trimmed := backend.TrimFairCarry(carry, nil, 2, 1)
	if len(trimmed) != 1 {
		t.Fatalf("trimmed locked flood = %d rows, want 1 (MaxPerInstance quota)", len(trimmed))
	}
	if trimmed[0].ID != 1 {
		t.Fatalf("trimmed[0] = %d, want FIFO-earliest 1", trimmed[0].ID)
	}

	// Locked extras stay dropped: a second instance's quota is independent.
	mixed := append(append([]backend.FairTaskRef(nil), carry...),
		backend.FairTaskRef{ID: 9001, InstanceID: "B", VisibleAt: base.Add(3000 * time.Millisecond)})
	trimmed = backend.TrimFairCarry(mixed, nil, 2, 1)
	if len(trimmed) != 2 {
		t.Fatalf("trimmed mixed = %d rows, want 2 (one quota per instance)", len(trimmed))
	}

	// Total bounded by the remaining limit, not by the sum of quotas.
	trimmed = backend.TrimFairCarry(mixed, nil, 1, 1)
	if len(trimmed) != 1 {
		t.Fatalf("trimmed to limit 1 = %d rows, want 1", len(trimmed))
	}

	// Already-full instances contribute nothing.
	secured := []backend.FairTaskRef{{ID: 1, InstanceID: "A"}}
	trimmed = backend.TrimFairCarry(carry[1:], secured, 2, 1)
	if len(trimmed) != 0 {
		t.Fatalf("trimmed over-quota carry = %d rows, want 0", len(trimmed))
	}
}

// TestNoteFairLossClearsOnRefill covers issue #294 round-17 P2(b): a
// historical lostLock bool stays true after a refill replaces every lost
// pick, causing a wasteful overflow rescan. Outstanding quota must clear once
// the securing pass restores the instance to the cap.
func TestNoteFairLossClearsOnRefill(t *testing.T) {
	outstanding := make(map[string]struct{})
	// Pass 1: A1 lost to a lock, B1 secured. A holds outstanding quota.
	backend.NoteFairLoss(outstanding,
		[]backend.FairTaskRef{{ID: 1, InstanceID: "A"}, {ID: 2, InstanceID: "B"}},
		map[int64]bool{2: true},
		[]backend.FairTaskRef{{ID: 2, InstanceID: "B"}}, 1)
	if _, ok := outstanding["A"]; !ok {
		t.Fatal("A should hold outstanding quota after losing its pick")
	}
	if _, ok := outstanding["B"]; ok {
		t.Fatal("B secured to the cap and must not hold outstanding quota")
	}

	// Refill secures A2: A is back at the cap, so the flag clears. Without
	// this, the next zero-pick carry pass would rescan the A suffix though no
	// A row is admissible.
	backend.NoteFairLoss(outstanding,
		[]backend.FairTaskRef{{ID: 3, InstanceID: "A"}},
		map[int64]bool{3: true},
		[]backend.FairTaskRef{{ID: 2, InstanceID: "B"}, {ID: 3, InstanceID: "A"}}, 1)
	if len(outstanding) != 0 {
		t.Fatalf("outstanding = %v, want empty after refill restores all quotas", outstanding)
	}
}

// TestNoteFairLossKeepsUnrefilledQuota covers the other half of P2(b): an
// unrefilled loss must keep the requery gate armed, or the dropped overflow
// tail stays stranded (Limit=4/MaxPerInstance=1 over A1,C1,B1,C2,B2.. with A1
// locked: pass 1 secures B1, the carry pass secures C2 with no current loss —
// only the outstanding prior loss admits A2 via requery).
func TestNoteFairLossKeepsUnrefilledQuota(t *testing.T) {
	outstanding := make(map[string]struct{})
	backend.NoteFairLoss(outstanding,
		[]backend.FairTaskRef{{ID: 1, InstanceID: "A"}, {ID: 2, InstanceID: "B"}},
		map[int64]bool{2: true},
		[]backend.FairTaskRef{{ID: 2, InstanceID: "B"}}, 1)
	// Carry pass secures C2 with no loss of its own: A is still outstanding.
	backend.NoteFairLoss(outstanding,
		[]backend.FairTaskRef{{ID: 3, InstanceID: "C"}},
		map[int64]bool{3: true},
		[]backend.FairTaskRef{{ID: 2, InstanceID: "B"}, {ID: 3, InstanceID: "C"}}, 1)
	if _, ok := outstanding["A"]; !ok {
		t.Fatalf("outstanding = %v, want A still armed (never refilled)", outstanding)
	}
}
