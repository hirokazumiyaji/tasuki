package dynamodb

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// The claim must gate on instance status in the SAME transaction as the
// lease update: a ConditionCheck on the instance row (status = running)
// plus the task lease Update. A separate post-claim GetItem leaves a race
// where a terminal commit lands between the lease update and the gate and
// the task is still delivered to the worker.
func TestClaimTransactItemsGatesInstanceRunning(t *testing.T) {
	visible := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	items := claimTransactItems("tasuki_wf_tasks", "tasuki_wf_instances", avS("WF#inst-1"), avS("inst-1"), 100, visible, "w1")
	if len(items) != 2 {
		t.Fatalf("claim transaction must hold exactly gate+lease items, got %d", len(items))
	}
	check := items[0].ConditionCheck
	if check == nil {
		t.Fatal("first transact item must be the instance-status ConditionCheck")
	}
	if aws.ToString(check.TableName) != "tasuki_wf_instances" {
		t.Fatalf("gate must target the instances table, got %q", aws.ToString(check.TableName))
	}
	if fromS(check.Key["id"]) != "inst-1" {
		t.Fatalf("gate must key the owning instance, got %v", check.Key)
	}
	if check.ExpressionAttributeNames["#s"] != "status" {
		t.Fatalf("gate must predicate on status, got %v", check.ExpressionAttributeNames)
	}
	found := false
	for _, v := range check.ExpressionAttributeValues {
		if fromS(v) == "running" {
			found = true
		}
	}
	if !found || aws.ToString(check.ConditionExpression) == "" {
		t.Fatalf("gate must require status running, got %q %v",
			aws.ToString(check.ConditionExpression), check.ExpressionAttributeValues)
	}
	upd := items[1].Update
	if upd == nil {
		t.Fatal("second transact item must be the lease Update")
	}
	if aws.ToString(upd.TableName) != "tasuki_wf_tasks" {
		t.Fatalf("lease update must target the tasks table, got %q", aws.ToString(upd.TableName))
	}
	if fromS(upd.Key["task_pk"]) != "WF#inst-1" {
		t.Fatalf("lease update must key the queried task row, got %v", upd.Key)
	}
	if aws.ToString(upd.ConditionExpression) != "visible_at = :old" {
		t.Fatalf("lease update must keep the visible_at guard, got %q", aws.ToString(upd.ConditionExpression))
	}
	if fromN(upd.ExpressionAttributeValues[":old"]) != 100 {
		t.Fatalf("lease guard must use the observed visible_at, got %v", upd.ExpressionAttributeValues[":old"])
	}
	if fromN(upd.ExpressionAttributeValues[":v"]) != timeToN(visible) {
		t.Fatalf("lease update must set the new visible_at, got %v", upd.ExpressionAttributeValues[":v"])
	}
	if fromS(upd.ExpressionAttributeValues[":w"]) != "w1" {
		t.Fatalf("lease update must set the worker, got %v", upd.ExpressionAttributeValues[":w"])
	}
}

// Only endpoints that do not implement TransactWriteItems at all may take
// the legacy lease path. A transaction that executed and cancelled already
// decided the claim and must never be reinterpreted as "unsupported".
func TestIsTransactionUnsupported(t *testing.T) {
	for _, err := range []error{
		errors.New("UnknownOperationException: transact not recognized"),
		errors.New("InvalidAction: TransactWriteItems is not supported by this store"),
		errors.New("501 Not Implemented"),
	} {
		if !isTransactionUnsupported(err) {
			t.Errorf("isTransactionUnsupported(%v) = false, want true", err)
		}
	}
	cancelled := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{{Code: aws.String("ConditionalCheckFailed")}},
	}
	executed := []error{
		nil,
		cancelled,
		&types.ConditionalCheckFailedException{},
		errors.New("ProvisionedThroughputExceededException: throttled"),
		errors.New("TransactionConflict: retry"),
	}
	for _, err := range executed {
		if isTransactionUnsupported(err) {
			t.Errorf("isTransactionUnsupported(%v) = true, want false", err)
		}
	}
}
