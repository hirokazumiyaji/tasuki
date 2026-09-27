package dynamodb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestBuildAdvancementItems_GatesRunningStatus covers Codex round-26 P1 on
// #291: the instance Update in the advancement transaction must condition on
// the persisted running status, not next_seq alone. A worker that loaded the
// workflow before TerminateInstance commits would otherwise commit
// activities/timers/journal/children post-termination. Fail-without-fix:
// revert the ConditionExpression to "next_seq = :expected" and both cases
// fail.
func TestBuildAdvancementItems_GatesRunningStatus(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		f := &fakeDynamo{
			getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
					"id": avS("inst-1"), "name": avS("WF"), "queue": avS("default"),
					"status": avS("running"), "next_seq": avN(2),
				}}, nil
			},
			updateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				return &dynamodb.UpdateItemOutput{Attributes: map[string]types.AttributeValue{"seq": avN(1)}}, nil
			},
		}
		b := newTestBackend(f)
		adv := backend.Advancement{InstanceID: "inst-1", TaskID: 7, ExpectedSeq: 2}
		if terminal {
			adv.Terminal = &backend.TerminalUpdate{Status: "completed", Result: []byte(`{}`)}
		} else {
			adv.ActivityTasks = []backend.NewTask{{InstanceID: "inst-1", Name: "act"}}
			adv.Timers = []backend.NewTimer{{Seq: 3, FireAt: time.Now().Add(time.Hour)}}
		}
		items, _, err := b.buildAdvancementItems(context.Background(), adv)
		if err != nil {
			t.Fatalf("terminal=%v: build: %v", terminal, err)
		}
		if len(items) == 0 || items[0].Update == nil {
			t.Fatalf("terminal=%v: first transact item must be the instance Update", terminal)
		}
		upd := items[0].Update
		if got := aws.ToString(upd.ConditionExpression); !strings.Contains(got, "next_seq = :expected") || !strings.Contains(got, "#status = :running") {
			t.Fatalf("terminal=%v: instance condition = %q, want next_seq + #status running gate", terminal, got)
		}
		if upd.ExpressionAttributeNames["#status"] != "status" {
			t.Fatalf("terminal=%v: names = %v, want #status=status", terminal, upd.ExpressionAttributeNames)
		}
		if fromS(upd.ExpressionAttributeValues[":running"]) != "running" {
			t.Fatalf("terminal=%v: missing :running=running value: %v", terminal, upd.ExpressionAttributeValues)
		}
	}
}

// TestFireDueTimers_FencesStatusInTransaction covers Codex round-26 P2 on
// #291: the running-path timer transaction must carry the instance-status
// ConditionCheck, not rely on the GetInstance pre-read. Fail-without-fix:
// drop the ConditionCheck and no transact item gates on status.
func TestFireDueTimers_FencesStatusInTransaction(t *testing.T) {
	var transacts []*dynamodb.TransactWriteItemsInput
	f := &fakeDynamo{
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
				"id": avS("inst-1"), "name": avS("WF"), "queue": avS("default"),
				"status": avS("running"), "next_seq": avN(2),
			}}, nil
		},
		queryFn: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			if aws.ToString(in.IndexName) == "fire_gsi" {
				return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
					{"instance_id": avS("inst-1"), "seq": avN(9)},
				}}, nil
			}
			return &dynamodb.QueryOutput{}, nil
		},
		updateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return &dynamodb.UpdateItemOutput{Attributes: map[string]types.AttributeValue{"seq": avN(1)}}, nil
		},
		transactFn: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			transacts = append(transacts, in)
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	b := newTestBackend(f)
	if _, err := b.FireDueTimers(context.Background(), 1); err != nil {
		t.Fatalf("fire: %v", err)
	}
	if len(transacts) != 1 {
		t.Fatalf("want 1 timer transaction, got %d", len(transacts))
	}
	gated := false
	for _, it := range transacts[0].TransactItems {
		if it.ConditionCheck == nil {
			continue
		}
		if aws.ToString(it.ConditionCheck.ConditionExpression) == "#s = :running" &&
			it.ConditionCheck.ExpressionAttributeNames["#s"] == "status" &&
			fromS(it.ConditionCheck.ExpressionAttributeValues[":running"]) == "running" {
			gated = true
		}
	}
	if !gated {
		t.Fatal("timer transaction carries no instance-status ConditionCheck (pre-read only)")
	}
}
