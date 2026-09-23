package dynamodb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestReleaseConditionRequiresClaimedGeneration is a regression test for
// the fenced-release finding: a renewal delayed past the lease (or a
// shutdown release racing a peer reclaim) let a stale holder clear the
// successor's lease by task key alone, so a third worker executed
// concurrently with the peer. The release condition must pin the claimed
// generation (numeric id on WF keys + claim ownership like the round-4
// renewal fence) and report ErrNotFound when the lease moved on.
func TestReleaseConditionRequiresClaimedGeneration(t *testing.T) {
	claimed := backend.Task{ID: 7, Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 3}
	cond, values := releaseCondition(claimed, 12345)
	for _, want := range []string{"attribute_exists(task_pk)", "id = :taskid", "worker_id = :wid", "attempt = :attempt"} {
		if !strings.Contains(cond, want) {
			t.Fatalf("release condition %q missing %q (stale holder could clear a successor's lease)", cond, want)
		}
	}
	if got := values[":v"]; !reflect.DeepEqual(got, avN(12345)) {
		t.Fatalf(":v=%v, want lease visibility", got)
	}
	if got := values[":taskid"]; !reflect.DeepEqual(got, avN(7)) {
		t.Fatalf(":taskid=%v, want claimed task id", got)
	}
	if got := values[":wid"]; !reflect.DeepEqual(got, avS("w1")) {
		t.Fatalf(":wid=%v, want claiming worker", got)
	}
	if got := values[":attempt"]; !reflect.DeepEqual(got, avN(3)) {
		t.Fatalf(":attempt=%v, want claimed attempt", got)
	}
	// A blank worker must not fence to an unclaimed item: the release
	// falls back to routing-only so legacy callers still release.
	cond, blank := releaseCondition(backend.Task{ID: 7, Kind: "activity"}, 0)
	if strings.Contains(cond, "worker_id") || strings.Contains(cond, "attempt") {
		t.Fatalf("blank-worker condition %q must not fence on ownership", cond)
	}
	if _, ok := blank[":wid"]; ok {
		t.Fatalf("blank-worker values must not bind :wid, got %v", blank)
	}
	// A zero numeric id on a WF key cannot pin the generation: the id
	// predicate is left out rather than matching id 0.
	cond, zeroid := releaseCondition(backend.Task{Kind: "workflow", InstanceID: "i1", WorkerID: "w1", Attempt: 3}, 0)
	if strings.Contains(cond, "id = :taskid") {
		t.Fatalf("zero-id condition %q must not pin a numeric id", cond)
	}
	if _, ok := zeroid[":taskid"]; ok {
		t.Fatalf("zero-id values must not bind :taskid, got %v", zeroid)
	}
}
