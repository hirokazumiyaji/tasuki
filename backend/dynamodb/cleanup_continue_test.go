package dynamodb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestCommitAdvancements_ContinuesPastCleanupFailure covers Codex round-19
// P2 on #291: a cleanup-retries-exhausted failure for one victim of a
// multi-instance batch must not skip later instances' sweeps or any parent
// ensure. The batch commits [inst-b (doomed cleanup), inst-a (healthy),
// both terminal with parent notifications] against a scripted fake whose
// task-table deletes fail only for inst-b. Old code returned B's error
// immediately: inst-a's residue was never swept and neither parent was
// ensured (dormant till the orphan scan).
func TestCommitAdvancements_ContinuesPastCleanupFailure(t *testing.T) {
	ctx := context.Background()
	const (
		instA = "inst-a"
		instB = "inst-b"
		parA  = "par-a"
		parB  = "par-b"
	)
	instances := map[string]string{instA: parA, instB: parB, parA: "", parB: ""}
	var mu sync.Mutex
	var deleted []string
	var transacts []*dynamodb.TransactWriteItemsInput
	f := &fakeDynamo{
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			table := *in.TableName
			if len(table) < len("tasuki_") {
				return &dynamodb.GetItemOutput{}, nil
			}
			if table[len("tasuki_"):] != "wf_instances" {
				return &dynamodb.GetItemOutput{}, nil
			}
			id := fromS(in.Key["id"])
			parent, ok := instances[id]
			if !ok {
				return &dynamodb.GetItemOutput{}, nil
			}
			item := map[string]types.AttributeValue{
				"id": avS(id), "name": avS("WF"), "queue": avS("default"),
				"status": avS("running"), "next_seq": avN(2),
			}
			if parent != "" {
				item["parent_id"] = avS(parent)
			}
			return &dynamodb.GetItemOutput{Item: item}, nil
		},
		queryFn: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			table := (*in.TableName)[len("tasuki_"):]
			id := ""
			if v, ok := in.ExpressionAttributeValues[":id"]; ok {
				id = fromS(v)
			}
			switch table {
			case "wf_tasks":
				// Instance-GSI sweep: each victim owns one workflow task.
				if id == instA || id == instB {
					return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
						{"task_pk": avS(wfTaskPK(id)), "instance_id": avS(id)},
					}}, nil
				}
				return &dynamodb.QueryOutput{}, nil
			case "wf_inbox":
				// Parent ensures see one pending event; cleanup sweeps see none.
				if id == parA || id == parB {
					return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{{"id": avN(1)}}}, nil
				}
				return &dynamodb.QueryOutput{}, nil
			default: // wf_signal_dedupe, wf_timers: nothing to sweep.
				return &dynamodb.QueryOutput{}, nil
			}
		},
		deleteItemFn: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			if pk, ok := in.Key["task_pk"]; ok && fromS(pk) == wfTaskPK(instB) {
				return nil, errors.New("boom: delete inst-b task")
			}
			mu.Lock()
			if pk, ok := in.Key["task_pk"]; ok {
				deleted = append(deleted, fromS(pk))
			}
			mu.Unlock()
			return &dynamodb.DeleteItemOutput{}, nil
		},
		updateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			// Inbox-seq allocation for the two parent notifications.
			return &dynamodb.UpdateItemOutput{Attributes: map[string]types.AttributeValue{"seq": avN(1)}}, nil
		},
		transactFn: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			mu.Lock()
			transacts = append(transacts, in)
			mu.Unlock()
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	b := newTestBackend(f)
	mkAdv := func(id string, taskID int64) backend.Advancement {
		return backend.Advancement{
			InstanceID:  id,
			TaskID:      taskID,
			ExpectedSeq: 2,
			ParentNotify: &journal.Event{
				Type:    journal.TypeSignalReceived,
				Name:    "child-done",
				RefSeq:  5,
				Payload: []byte(`{}`),
			},
			Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`{}`)},
		}
	}
	// Failing victim first: the old early return never reached inst-a or
	// either parent ensure.
	err := b.CommitAdvancements(ctx, []backend.Advancement{mkAdv(instB, 22), mkAdv(instA, 11)})
	if err == nil {
		t.Fatal("CommitAdvancements returned nil, want the retained inst-b cleanup error")
	}
	if !strings.Contains(err.Error(), instB) {
		t.Fatalf("returned error %v does not identify the failed victim %q", err, instB)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deleted) != 1 || deleted[0] != wfTaskPK(instA) {
		t.Fatalf("swept task deletes = %v, want only [%q] (later victim still swept)", deleted, wfTaskPK(instA))
	}
	// One combined commit plus one status-gated ensure per parent: the
	// ensures are exactly the 2-item [ConditionCheck, Put] transactions.
	ensured := map[string]bool{}
	commits := 0
	for _, tx := range transacts {
		if len(tx.TransactItems) > 2 {
			commits++
			continue
		}
		for _, item := range tx.TransactItems {
			if item.Put != nil {
				ensured[fromS(item.Put.Item["instance_id"])] = true
			}
		}
	}
	if commits != 1 {
		t.Fatalf("combined commit transactions = %d, want 1 (batch stayed atomic)", commits)
	}
	if !ensured[parA] || !ensured[parB] {
		t.Fatalf("parent ensures reached = %v, want both %q and %q despite the cleanup failure", ensured, parA, parB)
	}
}
