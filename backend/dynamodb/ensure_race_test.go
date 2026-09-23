package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func ensureTestEndpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("TASUKI_DYNAMODB_ENDPOINT not set")
	}
	return ep
}

func ensureTestBackend(t *testing.T) *Backend {
	t.Helper()
	b, err := New(context.Background(), Config{Endpoint: ensureTestEndpoint(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b
}

// taskRowPresent reports whether the singleton workflow task exists.
func taskRowPresent(t *testing.T, b *Backend, ctx context.Context, instanceID string) bool {
	t.Helper()
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(b.table("wf_tasks")),
		Key:            map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(instanceID))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(out.Item) != 0
}

// instanceTaskCount counts every task row of one instance via a
// strongly-consistent Scan.
func instanceTaskCount(t *testing.T, b *Backend, ctx context.Context, instanceID string) int {
	t.Helper()
	var start map[string]types.AttributeValue
	n := 0
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(b.table("wf_tasks")),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: start,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Items {
			if fromS(m["instance_id"]) == instanceID {
				n++
			}
		}
		if out.LastEvaluatedKey == nil {
			return n
		}
		start = out.LastEvaluatedKey
	}
}

// A stale ensure must not recreate the singleton workflow task after the
// terminal sweep deleted it (Codex round 7 on #328): CompleteActivity's
// inbox commit and the status read can both serialize before a terminal
// transition whose sweep then finishes before the task Put executes. The
// transactional Put (ConditionCheck: instance still running) aborts
// instead of undoing the cleanup. A bare PutItem recreates the row, which
// lingers with no later pass reaping it.
func TestPutWorkflowTaskIfRunning_NoRecreateAfterTerminal(t *testing.T) {
	ctx := context.Background()
	b := ensureTestBackend(t)

	const id = "ensure-race-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "k1"); err != nil {
		t.Fatal(err)
	}
	if !taskRowPresent(t, b, ctx, id) {
		t.Fatal("setup: expected a workflow task row before termination")
	}
	// Terminal transition with the full cleanup: the sweep deletes the
	// singleton task.
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	if taskRowPresent(t, b, ctx, id) {
		t.Fatal("setup: terminate must delete the workflow task row")
	}
	// The stale ensure's Put executes now, after the cleanup committed.
	if err := b.putWorkflowTaskIfRunning(ctx, id, workflowTaskItem(id, "default", newID(), nowUTC())); err != nil {
		t.Fatal(err)
	}
	if taskRowPresent(t, b, ctx, id) {
		t.Fatal("stale ensure recreated the workflow task after the terminal sweep deleted it")
	}
}

// The transactional ensure still creates the task for a running instance
// with pending inbox, and a second ensure is idempotent success.
func TestPutWorkflowTaskIfRunning_CreatesForRunning(t *testing.T) {
	ctx := context.Background()
	b := ensureTestBackend(t)

	const id = "ensure-race-2"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "k1"); err != nil {
		t.Fatal(err)
	}
	// Remove the task row directly (simulating a sweep) while the
	// instance stays running; the guarded Put must recreate it.
	if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(b.table("wf_tasks")),
		Key:       map[string]types.AttributeValue{"task_pk": avS(wfTaskPK(id))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.putWorkflowTaskIfRunning(ctx, id, workflowTaskItem(id, "default", newID(), nowUTC())); err != nil {
		t.Fatal(err)
	}
	if !taskRowPresent(t, b, ctx, id) {
		t.Fatal("guarded ensure must create the task for a running instance")
	}
	if err := b.putWorkflowTaskIfRunning(ctx, id, workflowTaskItem(id, "default", newID(), nowUTC())); err != nil {
		t.Fatal(err)
	}
}

// TerminateInstance must leave no task rows behind even with several
// activity tasks outstanding: it always runs the full (unbounded)
// cleanup, which is the backstop for the bounded per-completion sweep
// (see verifyTasksFirstPageByScan).
func TestTerminateInstanceFullTaskCleanup(t *testing.T) {
	ctx := context.Background()
	b := ensureTestBackend(t)

	const id = "terminate-cleanup-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "stage",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("stage workflow claim: %v n=%d", err, len(tasks))
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var acts []backend.NewTask
	for i := 0; i < 3; i++ {
		acts = append(acts, backend.NewTask{
			Kind: "activity", Queue: "default", InstanceID: id,
			Name: "w", Seq: st.NextSeq + int64(i), MaxAttempts: 1,
		})
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		ActivityTasks: acts,
	}); err != nil {
		t.Fatal(err)
	}
	if n := instanceTaskCount(t, b, ctx, id); n < 3 {
		t.Fatalf("setup: expected at least 3 task rows, got %d", n)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := instanceTaskCount(t, b, ctx, id); n != 0 {
		t.Fatalf("terminate left %d task rows behind, want 0", n)
	}
}
