package dynamodb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestSweepKeepsRow pins the terminal-sweep cutoff predicate (Codex
// round-22 P2 on #291): only rows created after the terminal transition
// survive; a zero cutoff (missing instance, legacy row without
// completed_at, retention purge) sweeps everything.
func TestSweepKeepsRow(t *testing.T) {
	const cutoff = int64(1_000_000)
	withCutoff := terminalSweepCutoff{cutoff: cutoff, hasCutoff: true}
	cases := []struct {
		name      string
		createdAt int64
		c         terminalSweepCutoff
		keep      bool
	}{
		{"pre-transition row sweeps", cutoff - 1, withCutoff, false},
		{"same-tick row sweeps", cutoff, withCutoff, false},
		{"post-transition racing send survives", cutoff + 1, withCutoff, true},
		{"zero cutoff sweeps all", cutoff + 1, terminalSweepCutoff{}, false},
		{"row without created_at sweeps", 0, withCutoff, false},
	}
	for _, c := range cases {
		if got := sweepKeepsRow(c.createdAt, c.c); got != c.keep {
			t.Errorf("%s: sweepKeepsRow = %v, want %v", c.name, got, c.keep)
		}
	}
}

// cutoffFake returns a fakeDynamo whose instance row carries completed_at=C
// and whose child tables each hold one pre-transition ("old") and one
// post-transition ("new") row for id. Deletes are recorded by table+key.
func cutoffFake(id string, completedAt int64) (*fakeDynamo, *sync.Mutex, map[string][]string) {
	var mu sync.Mutex
	deleted := map[string][]string{}
	old, new := completedAt-1000, completedAt+1000
	mkRow := func(key map[string]types.AttributeValue, created int64) map[string]types.AttributeValue {
		for k, v := range map[string]types.AttributeValue{
			"instance_id": avS(id), "created_at": avN(created),
		} {
			key[k] = v
		}
		return key
	}
	return &fakeDynamo{
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			table := (*in.TableName)[len("tasuki_"):]
			if table != "wf_instances" {
				return &dynamodb.GetItemOutput{}, nil
			}
			return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
				"id": avS(id), "name": avS("WF"), "queue": avS("default"),
				"status": avS("completed"), "next_seq": avN(3),
				"completed_at": avN(completedAt),
			}}, nil
		},
		queryFn: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			table := (*in.TableName)[len("tasuki_"):]
			switch table {
			case "wf_signal_dedupe":
				return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
					mkRow(map[string]types.AttributeValue{"dedupe_id": avS("old")}, old),
					mkRow(map[string]types.AttributeValue{"dedupe_id": avS("new")}, new),
				}}, nil
			case "wf_timers":
				return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
					mkRow(map[string]types.AttributeValue{"seq": avN(1)}, old),
					mkRow(map[string]types.AttributeValue{"seq": avN(2)}, new),
				}}, nil
			case "wf_inbox":
				return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
					mkRow(map[string]types.AttributeValue{"id": avN(11)}, old),
					mkRow(map[string]types.AttributeValue{"id": avN(12)}, new),
				}}, nil
			case "wf_tasks":
				return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
					mkRow(map[string]types.AttributeValue{"task_pk": avS("TASK#old")}, old),
					mkRow(map[string]types.AttributeValue{"task_pk": avS("TASK#new")}, new),
				}}, nil
			default:
				return &dynamodb.QueryOutput{}, nil
			}
		},
		deleteItemFn: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			table := (*in.TableName)[len("tasuki_"):]
			var key string
			switch table {
			case "wf_signal_dedupe":
				key = "dedupe:" + fromS(in.Key["dedupe_id"])
			case "wf_timers":
				key = "timer"
			case "wf_inbox":
				key = "inbox"
			case "wf_tasks":
				key = "task:" + fromS(in.Key["task_pk"])
			default:
				key = "other"
			}
			mu.Lock()
			deleted[table] = append(deleted[table], key)
			mu.Unlock()
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}, &mu, deleted
}

// TestCleanupTerminalInstance_PreservesPostCommitSends covers Codex
// round-22 P2 (a) on #291: a SendToInboxBatch that commits during the
// detached terminal cleanup must survive it. The sweep deletes only rows
// predating the terminal transition (dedupe marker and inbox row as a
// pair); old code swept every row, deleting the accepted send's inbox row
// after its dedupe phase had passed — losing the event and discarding
// every later send under the same DedupeID.
func TestCleanupTerminalInstance_PreservesPostCommitSends(t *testing.T) {
	ctx := context.Background()
	const id = "cutoff-inst"
	const completedAt = int64(1_700_000_000_000_000)
	f, mu, deleted := cutoffFake(id, completedAt)
	b := newTestBackend(f)

	if err := b.cleanupTerminalInstanceOnce(ctx, id); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// Pre-transition rows swept on every table...
	if n := len(deleted["wf_signal_dedupe"]); n != 1 || deleted["wf_signal_dedupe"][0] != "dedupe:old" {
		t.Errorf("dedupe deletes = %v, want only [dedupe:old]", deleted["wf_signal_dedupe"])
	}
	if n := len(deleted["wf_inbox"]); n != 1 {
		t.Errorf("inbox deletes = %d, want 1 (the pre-transition row)", n)
	}
	if n := len(deleted["wf_timers"]); n != 1 {
		t.Errorf("timer deletes = %d, want 1 (the pre-transition row)", n)
	}
	if n := len(deleted["wf_tasks"]); n != 1 || deleted["wf_tasks"][0] != "task:TASK#old" {
		t.Errorf("task deletes = %v, want only [task:TASK#old]", deleted["wf_tasks"])
	}
	// ...and the post-transition survivors are exactly the racing send.
	for _, table := range []string{"wf_signal_dedupe", "wf_inbox", "wf_timers", "wf_tasks"} {
		for _, k := range deleted[table] {
			if strings.Contains(k, "new") {
				t.Errorf("%s delete %q removed a post-transition row (accepted send lost)", table, k)
			}
		}
	}
}

// TestCleanupTerminalInstance_CutoffReadErrorRetries covers the error side
// of the cutoff: a transient instance-read failure must fail the attempt
// (retried by cleanupTerminalInstance) rather than sweep unbounded.
func TestCleanupTerminalInstance_CutoffReadErrorRetries(t *testing.T) {
	ctx := context.Background()
	f, _, _ := cutoffFake("cutoff-err", 1)
	f.getItemFn = func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return nil, errors.New("boom: throttled GetItem")
	}
	b := newTestBackend(f)
	if err := b.cleanupTerminalInstanceOnce(ctx, "cutoff-err"); err == nil {
		t.Fatal("cleanupTerminalInstanceOnce returned nil, want the cutoff read error (not an unbounded sweep)")
	}
	if n := len(f.deleted); n != 0 {
		t.Fatalf("sweep deleted %d rows despite the cutoff read failure, want none", n)
	}
}

// notifyTerminalSweepBackend returns a backend whose terminal cleanup always
// fails (task delete errors) while the commit itself succeeds, plus a
// subscribed terminal channel observing wake hints.
func notifyTerminalSweepBackend(id string) (*Backend, <-chan string) {
	f, _, _ := cutoffFake(id, 1_700_000_000_000_000)
	f.deleteItemFn = func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		if _, ok := in.Key["task_pk"]; ok {
			return nil, errors.New("boom: cleanup delete failed")
		}
		return &dynamodb.DeleteItemOutput{}, nil
	}
	// The advancement commit itself succeeds; the instance reads as
	// running so the terminal flip builds.
	f.getItemFn = func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		table := (*in.TableName)[len("tasuki_"):]
		if table != "wf_instances" {
			return &dynamodb.GetItemOutput{}, nil
		}
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
			"id": avS(id), "name": avS("WF"), "queue": avS("default"),
			"status": avS("running"), "next_seq": avN(2),
			"completed_at": avN(1_700_000_000_000_000),
		}}, nil
	}
	f.transactFn = func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
		return &dynamodb.TransactWriteItemsOutput{}, nil
	}
	b := newTestBackend(f)
	ctx := context.Background()
	ch, err := b.SubscribeTerminal(ctx)
	if err != nil {
		panic(err)
	}
	return b, ch
}

func awaitTerminal(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("terminal wake = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no terminal wake for %q (waiters left asleep despite the committed terminal state)", want)
	}
}

// TestCommitAdvancement_NotifiesTerminalDespiteCleanupError covers Codex
// round-22 P2 (b) on #291 (single-advancement path): a
// cleanup-retries-exhausted failure must still wake terminal waiters before
// the error surfaces. Old code returned the cleanup error before
// notifyAfterAdvancements, so cross-process Result waiters never woke.
func TestCommitAdvancement_NotifiesTerminalDespiteCleanupError(t *testing.T) {
	ctx := context.Background()
	const id = "notify-single"
	b, ch := notifyTerminalSweepBackend(id)
	result := []byte(`"ok"`)
	err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: 7, ExpectedSeq: 2,
		NewEvents: []journal.Event{{Seq: 2, Type: journal.TypeWorkflowCompleted, Payload: result}},
		Terminal:  &backend.TerminalUpdate{Status: "completed", Result: result},
	})
	if err == nil {
		t.Fatal("CommitAdvancement returned nil, want the retained cleanup error")
	}
	awaitTerminal(t, ch, id)
}

// TestCommitAdvancements_NotifiesTerminalsDespiteCleanupError covers the
// same ordering on the combined-batch path.
func TestCommitAdvancements_NotifiesTerminalsDespiteCleanupError(t *testing.T) {
	ctx := context.Background()
	const idA = "notify-batch-a"
	const idB = "notify-batch-b"
	b, ch := notifyTerminalSweepBackend(idB)
	// notifyTerminalSweepBackend answers every instance read with the same
	// running-instance shape, so idA's preflight/build reads succeed too;
	// only its TaskID differs, and every task delete fails.
	mkAdv := func(id string, taskID int64) backend.Advancement {
		result := []byte(`"ok"`)
		return backend.Advancement{
			InstanceID: id, TaskID: taskID, ExpectedSeq: 2,
			NewEvents: []journal.Event{{Seq: 2, Type: journal.TypeWorkflowCompleted, Payload: result}},
			Terminal:  &backend.TerminalUpdate{Status: "completed", Result: result},
		}
	}
	// Two terminal victims: the combined commit succeeds, every per-victim
	// cleanup fails on the task delete, and both terminal wakes must still
	// fire before the retained error surfaces.
	err := b.CommitAdvancements(ctx, []backend.Advancement{mkAdv(idA, 11), mkAdv(idB, 22)})
	if err == nil {
		t.Fatal("CommitAdvancements returned nil, want the retained cleanup error")
	}
	awaitTerminal(t, ch, idA)
	awaitTerminal(t, ch, idB)
}
