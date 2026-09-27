package dynamodb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestActivityRenewalFenceRequiresClaimedGeneration is the regression test
// for round-28 P2b (fence activity renewals on DynamoDB): a delayed
// activity renewal from a stale holder that landed after a peer reclaim
// + retry updated visible_at by ID alone, replacing the peer's retry
// delay with a full lease and hiding the task. The renewal must be
// conditioned on the claimed generation (worker + attempt), mirroring
// the relational/memory fencing from round-20, with ErrNotFound on
// mismatch. Without the fix the condition is empty (ID-only update).
func TestActivityRenewalFenceRequiresClaimedGeneration(t *testing.T) {
	claimed := backend.Task{ID: 42, Kind: "activity", WorkerID: "w1", Attempt: 3}
	cond, values := activityRenewalFence(claimed, 12345)
	for _, want := range []string{"worker_id = :w", "attempt = :a"} {
		if !strings.Contains(cond, want) {
			t.Fatalf("activity renewal condition %q missing %q (stale holder could overwrite a peer retry delay)", cond, want)
		}
	}
	if got := values[":w"]; !reflect.DeepEqual(got, avS("w1")) {
		t.Fatalf(":w=%v, want claiming worker", got)
	}
	if got := values[":a"]; !reflect.DeepEqual(got, avN(3)) {
		t.Fatalf(":a=%v, want claimed attempt", got)
	}
	if got := values[":v"]; !reflect.DeepEqual(got, avN(12345)) {
		t.Fatalf(":v=%v, want lease visibility", got)
	}
}

// TestActivityRenewalFenceLegacyFallback covers the empty-WorkerID
// fallback: older/test callers that pass only an ID still renew via the
// routing-only update instead of matching nothing.
func TestActivityRenewalFenceLegacyFallback(t *testing.T) {
	legacy := backend.Task{ID: 42, Kind: "activity"}
	cond, _ := activityRenewalFence(legacy, 12345)
	if cond != "" {
		t.Fatalf("legacy renewal condition = %q, want empty (routing-only fallback)", cond)
	}
}
