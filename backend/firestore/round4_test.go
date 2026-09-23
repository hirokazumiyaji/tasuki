package firestore

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestReleaseTaskDocIDRoutesByKind is a regression test for the shutdown
// release routing finding: the abandon path passed only the numeric ID to
// ReleaseLease, which addresses a missing ACT# document for WF# workflow
// tasks (ErrNotFound) and stalls peers until lease expiry. Releases must
// route by kind like ExtendLease/NackTask.
func TestReleaseTaskDocIDRoutesByKind(t *testing.T) {
	wf := backend.Task{ID: 123, Kind: "workflow", InstanceID: "i1"}
	if got := releaseTaskDocID(wf); got != wfTaskID("i1") {
		t.Fatalf("workflow release doc=%q, want %q (WF# instance doc)", got, wfTaskID("i1"))
	}
	act := backend.Task{ID: 456, Kind: "activity", InstanceID: "i1"}
	if got := releaseTaskDocID(act); got != actTaskID(456) {
		t.Fatalf("activity release doc=%q, want %q", got, actTaskID(456))
	}
}

// TestCheckWorkflowRenewalDocFencesGeneration is a regression test for the
// renewal fencing finding: renewal accepted any document on the instance
// workflow key, so an EnsureWorkflowTask replacement (new numeric id) plus
// a late renewal extended the successor. Renewal must apply only while the
// document still carries the claimed generation (id + claim ownership).
func TestCheckWorkflowRenewalDocFencesGeneration(t *testing.T) {
	claimed := backend.Task{ID: 9, Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 2}
	good := map[string]any{"id": int64(9), "worker_id": "w1", "attempt": int64(2)}
	if err := checkWorkflowRenewalDoc(good, claimed); err != nil {
		t.Fatalf("claimed generation rejected: %v", err)
	}
	bads := map[string]map[string]any{
		"successor id":    {"id": int64(10), "worker_id": "w1", "attempt": int64(2)},
		"peer reclaim":    {"id": int64(9), "worker_id": "w2", "attempt": int64(3)},
		"attempt bump":    {"id": int64(9), "worker_id": "w1", "attempt": int64(3)},
		"unclaimed fresh": {"id": int64(10)},
		"missing":         nil,
	}
	for name, doc := range bads {
		if err := checkWorkflowRenewalDoc(doc, claimed); err == nil {
			t.Fatalf("%s: stale renewal accepted (would extend a successor)", name)
		}
	}
}
