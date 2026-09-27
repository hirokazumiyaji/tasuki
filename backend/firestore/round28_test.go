package firestore

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestCheckActivityRenewalDocFencesGeneration is the regression test for
// round-28 P2b (fence activity renewals on Firestore): a delayed renewal
// from a stale holder that landed after a peer reclaim + retry replaced
// the peer's retry delay with a full lease. The renewal must be rejected
// with ErrNotFound when the document no longer carries the claimed
// generation (worker + attempt), mirroring the relational/memory fencing
// from round-20. Without the fix the renewal updates by ID alone.
func TestCheckActivityRenewalDocFencesGeneration(t *testing.T) {
	claimed := backend.Task{ID: 7, Kind: "activity", WorkerID: "w1", Attempt: 2}
	live := map[string]any{"worker_id": "w1", "attempt": int64(2)}
	if err := checkActivityRenewalDoc(live, claimed); err != nil {
		t.Fatalf("live renewal doc = %v, want nil", err)
	}
	reclaimed := map[string]any{"worker_id": "w2", "attempt": int64(3)}
	if err := checkActivityRenewalDoc(reclaimed, claimed); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("reclaimed renewal doc = %v, want ErrNotFound (stale holder must not extend the peer lease)", err)
	}
	if err := checkActivityRenewalDoc(nil, claimed); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("nil renewal doc = %v, want ErrNotFound", err)
	}
}

// TestCheckActivityRenewalDocLegacyFallback covers the empty-WorkerID
// fallback: older/test callers that pass only an ID still renew.
func TestCheckActivityRenewalDocLegacyFallback(t *testing.T) {
	legacy := backend.Task{ID: 7, Kind: "activity"}
	other := map[string]any{"worker_id": "w9", "attempt": int64(9)}
	if err := checkActivityRenewalDoc(other, legacy); err != nil {
		t.Fatalf("legacy renewal doc = %v, want nil (routing-only fallback)", err)
	}
}
