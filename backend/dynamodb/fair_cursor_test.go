package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TestClaimResumeKey_CarriesTableAndIndexKeys pins the intra-page resume
// contract: refilling after the last examined item must resume the claim_gsi
// Query right after it (table HASH key plus GSI HASH+RANGE keys), so the
// unexamined page suffix is re-examined instead of skipped.
func TestClaimResumeKey_CarriesTableAndIndexKeys(t *testing.T) {
	item := map[string]types.AttributeValue{
		"task_pk": avS("ACT#7"), "id": avN(7), "gsi_pk": avS("claim/activity/q"),
		"visible_at": avN(123), "instance_id": avS("inst-1"),
	}
	key := claimResumeKey(item)
	if key == nil {
		t.Fatal("claimResumeKey = nil, want table+GSI keys")
	}
	if len(key) != 3 {
		t.Fatalf("claimResumeKey has %d attrs, want 3 (task_pk, gsi_pk, visible_at)", len(key))
	}
	if fromS(key["task_pk"]) != "ACT#7" || fromS(key["gsi_pk"]) != "claim/activity/q" || fromN(key["visible_at"]) != 123 {
		t.Fatalf("claimResumeKey = %v, want the item's table+GSI keys", key)
	}
	if _, ok := key["id"]; ok {
		t.Fatalf("claimResumeKey must not carry non-key attrs: %v", key)
	}
	if claimResumeKey(map[string]types.AttributeValue{"task_pk": avS("x")}) != nil {
		t.Fatal("claimResumeKey without GSI keys must be nil (caller re-fetches the page)")
	}
}
