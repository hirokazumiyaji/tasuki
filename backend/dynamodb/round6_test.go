package dynamodb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestReleaseFenceRequiresClaimedGeneration is a regression test for the
// fenced-release finding: a renewal delayed past the lease (or a shutdown
// release racing a peer reclaim) let a stale holder clear the successor's
// lease by task key alone, so a third worker executed concurrently with
// the peer. The release condition must pin the claimed generation (id +
// claim ownership like the round-4 renewal fence) and report ErrNotFound
// when the lease moved on.
func TestReleaseFenceRequiresClaimedGeneration(t *testing.T) {
	claimed := backend.Task{ID: 7, Kind: "activity", InstanceID: "i1", WorkerID: "w1", Attempt: 3}
	cond, values := releaseFence(claimed, 12345)
	for _, want := range []string{"attribute_exists(task_pk)", "id = :id", "worker_id = :w", "attempt = :a"} {
		if !strings.Contains(cond, want) {
			t.Fatalf("release condition %q missing %q (stale holder could clear a successor's lease)", cond, want)
		}
	}
	if got := values[":v"]; !reflect.DeepEqual(got, avN(12345)) {
		t.Fatalf(":v=%v, want lease visibility", got)
	}
	if got := values[":id"]; !reflect.DeepEqual(got, avN(7)) {
		t.Fatalf(":id=%v, want claimed task id", got)
	}
	if got := values[":w"]; !reflect.DeepEqual(got, avS("w1")) {
		t.Fatalf(":w=%v, want claiming worker", got)
	}
	if got := values[":a"]; !reflect.DeepEqual(got, avN(3)) {
		t.Fatalf(":a=%v, want claimed attempt", got)
	}
	// A blank identity must not fence to an unclaimed item: the zero
	// worker never matches a claimed lease.
	_, blank := releaseFence(backend.Task{ID: 7, Kind: "activity"}, 0)
	if !reflect.DeepEqual(blank[":w"], avS("")) {
		t.Fatalf(":w=%v, want empty claiming worker for a blank identity", blank[":w"])
	}
}
