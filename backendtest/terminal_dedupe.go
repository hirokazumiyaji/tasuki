package backendtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// largeDedupeCount exceeds Firestore's 500-write transaction limit: any
// terminal path that deletes one row per dedupe key inside a single commit
// fails once an instance accumulates this many keys (issue #296).
const largeDedupeCount = 600

// testTerminalLargeDedupe verifies that TerminateInstance and terminal
// advancements succeed on instances with >500 dedupe rows. Dedupe cleanup
// must live outside the committing transaction (paged post-commit sweep,
// purge as backstop) instead of scaling one commit with the row count.
func testTerminalLargeDedupe(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()

	// Terminate path.
	b := newBackend(t)
	termID := instanceID("bigterm-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: termID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	sendManyDeduped(t, b, termID, largeDedupeCount)
	if st, err := b.LoadWorkflow(ctx, termID); err != nil {
		t.Fatal(err)
	} else if len(st.Inbox) != largeDedupeCount {
		t.Fatalf("terminate setup: inbox=%d want %d", len(st.Inbox), largeDedupeCount)
	}
	if err := b.TerminateInstance(ctx, termID); err != nil {
		t.Fatalf("TerminateInstance with %d dedupe rows: %v", largeDedupeCount, err)
	}
	inst, err := b.GetInstance(ctx, termID)
	if err != nil || inst.Status != "terminated" {
		t.Fatalf("status after terminate: %v %#v", err, inst)
	}
	// Dedupe keys must be gone: re-sending a previously seen key inserts anew.
	before := inboxLen(t, b, termID)
	if err := b.SendToInbox(ctx, termID,
		journal.Event{Type: journal.TypeSignalReceived, Name: "again"}, "k-0000"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen(t, b, termID); got != before+1 {
		t.Fatalf("dedupe not cleared by terminate: inbox %d -> %d, want +1", before, got)
	}

	// Terminal advancement path (normal completion with leftover dedupe keys).
	b2 := newBackend(t)
	doneID := instanceID("bigdone-", t)
	if err := b2.CreateInstance(ctx, backend.NewInstance{ID: doneID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	sendManyDeduped(t, b2, doneID, largeDedupeCount)
	tasks, err := b2.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Second, WorkerID: "bigdedupe",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %s: %v %#v", doneID, err, tasks)
	}
	st, err := b2.LoadWorkflow(ctx, doneID)
	if err != nil {
		t.Fatal(err)
	}
	// Minimal terminal turn: no inbox drain, so the only unbounded cost is the
	// dedupe sweep itself. Before the fix this commit buffered one delete per
	// dedupe key and breached the write cap.
	termSeq := st.NextSeq
	if err := b2.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  doneID,
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq: termSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`),
		}},
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
	}); err != nil {
		t.Fatalf("terminal CommitAdvancement with %d dedupe rows: %v", largeDedupeCount, err)
	}
	if inst, err := b2.GetInstance(ctx, doneID); err != nil || inst.Status != "completed" {
		t.Fatalf("status after terminal commit: %v %#v", err, inst)
	}
	before = inboxLen(t, b2, doneID)
	if err := b2.SendToInbox(ctx, doneID,
		journal.Event{Type: journal.TypeSignalReceived, Name: "again"}, "k-0000"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen(t, b2, doneID); got != before+1 {
		t.Fatalf("dedupe not cleared by terminal commit: inbox %d -> %d, want +1", before, got)
	}
}

func sendManyDeduped(t *testing.T, b backend.Backend, id string, n int) {
	t.Helper()
	ctx := context.Background()
	batch := backend.InboxBatchLimit(b.Capabilities())
	if batch <= 0 {
		batch = 100
	}
	for start := 0; start < n; start += batch {
		end := start + batch
		if end > n {
			end = n
		}
		items := make([]backend.InboxItem, 0, end-start)
		for j := start; j < end; j++ {
			items = append(items, backend.InboxItem{
				Event:    journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(fmt.Sprintf(`{"n":%d}`, j))},
				DedupeID: fmt.Sprintf("k-%04d", j),
			})
		}
		if err := b.SendToInboxBatch(ctx, id, items); err != nil {
			t.Fatalf("send batch [%d,%d): %v", start, end, err)
		}
	}
}

func inboxLen(t *testing.T, b backend.Backend, id string) int {
	t.Helper()
	st, err := b.LoadWorkflow(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return len(st.Inbox)
}
