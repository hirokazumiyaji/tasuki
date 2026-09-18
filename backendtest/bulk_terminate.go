package backendtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Bulk Terminate/Purge (#299 item 5).
//
// An instance holding more child rows than a single backend write allows
// (Firestore ~500 writes per transaction, Spanner mutation limits) must still
// terminate and purge completely. Backends that sweep children in one
// unchunked write fail here; they skip until chunked deletes land.
func testBulkTerminatePurge(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	if !b.Capabilities().SupportsBulkCleanup {
		t.Skip("chunked bulk terminate/purge not implemented (see #299: Firestore 500-write cap, Spanner mutation limits)")
	}
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	setNow(b, base)
	id := instanceID("bulk-", t)

	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}

	// 600 inbox rows (half with dedupe IDs), sent in capability-sized batches.
	const total = 600
	batchLimit := backend.InboxBatchLimit(b.Capabilities())
	for start := 0; start < total; start += batchLimit {
		end := start + batchLimit
		if end > total {
			end = total
		}
		items := make([]backend.InboxItem, 0, end-start)
		for i := start; i < end; i++ {
			item := backend.InboxItem{
				Event: journal.Event{
					Type: journal.TypeSignalReceived, Name: fmt.Sprintf("sig-%d", i),
					Payload: []byte(`{}`),
				},
			}
			if i%2 == 0 {
				item.DedupeID = fmt.Sprintf("bulk-pay-%d", i)
			}
			items = append(items, item)
		}
		if err := b.SendToInboxBatch(ctx, id, items); err != nil {
			t.Fatal(err)
		}
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != total {
		t.Fatalf("inbox=%d want %d", len(st.Inbox), total)
	}

	// A few activity tasks and timers ride along for the sweep.
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "bulk",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim wf: %v %#v", err, tasks)
	}
	st, err = b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	adv := backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		DrainedInbox: inboxIDs(st.Inbox),
	}
	for i := 0; i < 10; i++ {
		seq := st.NextSeq + int64(i)
		adv.NewEvents = append(adv.NewEvents, journal.Event{
			Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
		})
		adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
			Kind: "activity", Queue: "default", InstanceID: id,
			Name: "step", Seq: seq, Input: []byte(`{}`),
		})
		adv.Timers = append(adv.Timers, backend.NewTimer{
			Seq: seq, FireAt: base.Add(time.Hour),
		})
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatal(err)
	}

	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatalf("TerminateInstance with %d child rows: %v", total, err)
	}
	st, err = b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 0 {
		t.Fatalf("inbox=%d after terminate, want 0", len(st.Inbox))
	}
	for _, kind := range []string{"activity", "workflow"} {
		claimed, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: kind, Queues: []string{"default"}, Limit: 100,
			Lease: time.Minute, WorkerID: "bulk-check",
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range claimed {
			if task.InstanceID == id {
				t.Fatalf("%s task %d survived bulk terminate", kind, task.ID)
			}
		}
	}

	older := time.Hour
	if c, ok := b.(ClockSetter); ok {
		c.SetNow(base.Add(2 * time.Hour))
	} else {
		older = 0 // real-time stores mark completions at wall-clock now
	}
	n, err := b.PurgeInstances(ctx, older, nil, 0)
	if err != nil {
		t.Fatalf("PurgeInstances with %d child rows: %v", total, err)
	}
	if n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("want ErrNotFound after purge, got %v", err)
	}
}

func inboxIDs(inbox []backend.InboxEvent) []int64 {
	ids := make([]int64, 0, len(inbox))
	for _, item := range inbox {
		ids = append(ids, item.ID)
	}
	return ids
}
