package backendtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// CommitAdvancements all-or-nothing (#299 item 3, see #296).
//
// A batch containing one conflicting advancement must leave every instance
// untouched. Case A mixes two instances (good + stale ExpectedSeq); case B
// advances the same instance twice in one batch, which must also fail
// atomically: the sequentially-committing implementation applied the first
// advancement before observing the second conflict (memory did this before
// the preflight fix).
func testCommitAdvancementsAtomic(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	batcher, ok := b.(backend.AdvancementBatcher)
	if !ok {
		t.Skip("backend does not implement CommitAdvancements (see #296)")
	}
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	claim := func() map[string]backend.Task {
		t.Helper()
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{"default"}, Limit: 10,
			Lease: time.Minute, WorkerID: "batch-atomic",
		})
		if err != nil {
			t.Fatal(err)
		}
		byID := map[string]backend.Task{}
		for _, task := range tasks {
			byID[task.InstanceID] = task
		}
		return byID
	}
	snapshot := func(id string) (int64, int) {
		t.Helper()
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return st.NextSeq, len(st.Journal)
	}
	terminalAdv := func(id string, taskID, seq int64) backend.Advancement {
		return backend.Advancement{
			InstanceID:  id,
			TaskID:      taskID,
			ExpectedSeq: seq,
			NewEvents: []journal.Event{{
				Seq: seq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`),
			}},
			Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
		}
	}

	for _, id := range []string{"atomic-a", "atomic-b", "atomic-c"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	byID := claim()
	if len(byID) != 3 {
		t.Fatalf("want 3 workflow tasks, got %d", len(byID))
	}
	seqA, journalA := snapshot("atomic-a")
	seqB, journalB := snapshot("atomic-b")

	// Case A: one good advancement plus one with a stale ExpectedSeq.
	good := terminalAdv("atomic-a", byID["atomic-a"].ID, seqA)
	bad := terminalAdv("atomic-b", byID["atomic-b"].ID, seqB-1)
	if err := batcher.CommitAdvancements(ctx, []backend.Advancement{good, bad}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("mixed batch: want ErrConflict, got %v", err)
	}
	for _, id := range []string{"atomic-a", "atomic-b"} {
		inst, err := b.GetInstance(ctx, id)
		if err != nil || inst.Status != "running" {
			t.Fatalf("mixed batch: %s status=%v err=%v (want running)", id, inst, err)
		}
	}
	if seq, n := snapshot("atomic-a"); seq != seqA || n != journalA {
		t.Fatalf("mixed batch: atomic-a moved (%d/%d vs %d/%d)", seq, n, seqA, journalA)
	}
	if seq, n := snapshot("atomic-b"); seq != seqB || n != journalB {
		t.Fatalf("mixed batch: atomic-b moved (%d/%d vs %d/%d)", seq, n, seqB, journalB)
	}

	// Case B: two advancements for the same instance in one batch.
	seqC, journalC := snapshot("atomic-c")
	dup1 := terminalAdv("atomic-c", byID["atomic-c"].ID, seqC)
	dup2 := terminalAdv("atomic-c", byID["atomic-c"].ID, seqC)
	if err := batcher.CommitAdvancements(ctx, []backend.Advancement{dup1, dup2}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("duplicate batch: want ErrConflict, got %v", err)
	}
	inst, err := b.GetInstance(ctx, "atomic-c")
	if err != nil || inst.Status != "running" {
		t.Fatalf("duplicate batch: atomic-c status=%v err=%v (want running)", inst, err)
	}
	if seq, n := snapshot("atomic-c"); seq != seqC || n != journalC {
		t.Fatalf("duplicate batch: atomic-c moved (%d/%d vs %d/%d)", seq, n, seqC, journalC)
	}

	// Case C: an oversized distinct-instance batch with one stale
	// advancement must apply nothing (#299 item: DynamoDB sequential
	// fallback). Each advancement carries enough journal events that the
	// combined DynamoDB transaction (2 + events per advancement) exceeds
	// the 100-item TransactWriteItems limit, so a store that falls back to
	// sequential commits would apply the good advancement before observing
	// the stale ExpectedSeq. The batch must fail with every instance
	// untouched: stores with a combined size cap reject with a sizing error
	// before applying anything, while unbounded stores (single SQL
	// transaction, preflighted memory batch) fail the stale advancement
	// with ErrConflict — either way nothing may move.
	for _, id := range []string{"atomic-d", "atomic-e"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	tasksDE, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "batch-atomic",
	})
	if err != nil {
		t.Fatal(err)
	}
	taskDE := map[string]backend.Task{}
	for _, task := range tasksDE {
		taskDE[task.InstanceID] = task
	}
	if len(taskDE) != 2 {
		t.Fatalf("want 2 workflow tasks for atomic-d/e, got %d", len(taskDE))
	}
	bigAdv := func(id string, seq int64, n int) backend.Advancement {
		events := make([]journal.Event, 0, n)
		for i := 0; i < n; i++ {
			events = append(events, journal.Event{
				Seq: seq + int64(i), Type: journal.TypeSignalReceived, Name: "pad", Payload: []byte(`"pad"`),
			})
		}
		return backend.Advancement{
			InstanceID: id, TaskID: taskDE[id].ID, ExpectedSeq: seq, NewEvents: events,
		}
	}
	seqD, journalD := snapshot("atomic-d")
	seqE, journalE := snapshot("atomic-e")
	// 2 + 60 events per advancement = 62 DynamoDB ops each, 124 combined.
	bigGood := bigAdv("atomic-d", seqD, 60)
	bigStale := bigAdv("atomic-e", seqE-1, 60)
	if err := batcher.CommitAdvancements(ctx, []backend.Advancement{bigGood, bigStale}); err == nil {
		t.Fatal("oversized conflicting batch: want error, got nil")
	}
	for _, id := range []string{"atomic-d", "atomic-e"} {
		inst, err := b.GetInstance(ctx, id)
		if err != nil || inst.Status != "running" {
			t.Fatalf("oversized conflicting batch: %s status=%v err=%v (want running)", id, inst, err)
		}
	}
	if seq, n := snapshot("atomic-d"); seq != seqD || n != journalD {
		t.Fatalf("oversized conflicting batch: atomic-d moved (%d/%d vs %d/%d): partial application", seq, n, seqD, journalD)
	}
	if seq, n := snapshot("atomic-e"); seq != seqE || n != journalE {
		t.Fatalf("oversized conflicting batch: atomic-e moved (%d/%d vs %d/%d): partial application", seq, n, seqE, journalE)
	}
}
