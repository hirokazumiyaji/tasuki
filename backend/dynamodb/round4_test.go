package dynamodb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestReleaseTaskPKRoutesByKind is a regression test for the shutdown
// release routing finding: the abandon path passed only the numeric ID to
// ReleaseLease, which addresses a missing ACT# key for WF# workflow tasks
// (ErrNotFound) and stalls peers until lease expiry. Releases must route
// by kind like ExtendLease/NackTask.
func TestReleaseTaskPKRoutesByKind(t *testing.T) {
	wf := backend.Task{ID: 123, Kind: "workflow", InstanceID: "i1"}
	if got := releaseTaskPK(wf); got != wfTaskPK("i1") {
		t.Fatalf("workflow release key=%q, want %q (WF# instance key)", got, wfTaskPK("i1"))
	}
	if got := releaseTaskPK(wf); got == actTaskPK(123) {
		t.Fatalf("workflow release key=%q addresses the ACT# key (ErrNotFound)", got)
	}
	act := backend.Task{ID: 456, Kind: "activity", InstanceID: "i1"}
	if got := releaseTaskPK(act); got != actTaskPK(456) {
		t.Fatalf("activity release key=%q, want %q", got, actTaskPK(456))
	}
}

// TestWorkflowRenewalFenceRequiresClaimedGeneration is a regression test
// for the renewal fencing finding: renewal accepted any item on the
// instance workflow key, so an EnsureWorkflowTask replacement (new numeric
// id) plus a late renewal extended the successor. The renewal condition
// must pin the claimed generation (id + claim ownership).
func TestWorkflowRenewalFenceRequiresClaimedGeneration(t *testing.T) {
	claimed := backend.Task{ID: 9, Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 2}
	cond, values := workflowRenewalFence(claimed, 12345)
	for _, want := range []string{"attribute_exists(task_pk)", "id = :id", "worker_id = :w", "attempt = :a"} {
		if !strings.Contains(cond, want) {
			t.Fatalf("renewal condition %q missing %q (stale holder could extend a successor)", cond, want)
		}
	}
	if got := values[":v"]; !reflect.DeepEqual(got, avN(12345)) {
		t.Fatalf(":v=%v, want lease visibility", got)
	}
	if got := values[":id"]; !reflect.DeepEqual(got, avN(9)) {
		t.Fatalf(":id=%v, want claimed task id", got)
	}
	if got := values[":w"]; !reflect.DeepEqual(got, avS("w1")) {
		t.Fatalf(":w=%v, want claiming worker", got)
	}
	if got := values[":a"]; !reflect.DeepEqual(got, avN(2)) {
		t.Fatalf(":a=%v, want claimed attempt", got)
	}
}
