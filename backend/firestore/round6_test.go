package firestore

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestCheckReleaseDocFencesGeneration is a regression test for the
// fenced-release finding: a renewal delayed past the lease (or a shutdown
// release racing a peer reclaim) let a stale holder clear the successor's
// lease by document key alone, so a third worker executed concurrently
// with the peer. The release must apply only while the document still
// carries the claimed generation (numeric id on WF keys + claim
// ownership, mirroring the round-4 renewal fence) and report ErrNotFound
// when the lease moved on.
func TestCheckReleaseDocFencesGeneration(t *testing.T) {
	claimed := backend.Task{ID: 7, Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 3}
	good := map[string]any{"id": int64(7), "worker_id": "w1", "attempt": int64(3)}
	if err := checkReleaseDoc(good, claimed); err != nil {
		t.Fatalf("claimed generation rejected: %v", err)
	}
	bads := map[string]map[string]any{
		"successor id":       {"id": int64(8), "worker_id": "w1", "attempt": int64(3)},
		"peer reclaim":       {"id": int64(7), "worker_id": "w2", "attempt": int64(4)},
		"same worker rebump": {"id": int64(7), "worker_id": "w1", "attempt": int64(4)},
		"missing":            nil,
	}
	for name, doc := range bads {
		if err := checkReleaseDoc(doc, claimed); err == nil {
			t.Fatalf("%s: stale release accepted (would clear a successor's lease)", name)
		}
	}
	// Empty token fields fall back to routing-only so legacy callers still
	// release: a zero id never pins, and a blank worker never fences.
	zeroID := backend.Task{Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 3}
	if err := checkReleaseDoc(map[string]any{"id": int64(99), "worker_id": "w1", "attempt": int64(3)}, zeroID); err != nil {
		t.Fatalf("zero-id claim rejected: %v", err)
	}
	blankWorker := backend.Task{ID: 7, Kind: "activity", InstanceID: "i1"}
	if err := checkReleaseDoc(map[string]any{"id": int64(7), "worker_id": "w9", "attempt": int64(9)}, blankWorker); err != nil {
		t.Fatalf("blank-worker claim rejected: %v", err)
	}
}
