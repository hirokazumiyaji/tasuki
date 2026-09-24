package spanner

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// countChildRows counts rows of one instance in a child table (test helper
// for asserting sweep outcomes).
func countChildRows(t *testing.T, b *Backend, ctx context.Context, table, id string) int {
	t.Helper()
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    fmt.Sprintf(`SELECT COUNT(*) AS n FROM %s WHERE instance_id = @id`, table),
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := row.Columns(&n); err != nil {
		t.Fatal(err)
	}
	return int(n)
}

func readSweepTsForTest(t *testing.T, b *Backend, ctx context.Context, id string) time.Time {
	t.Helper()
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"sweep_commit_ts"})
	if err != nil {
		t.Fatal(err)
	}
	var sweepTs spanner.NullTime
	if err := row.Columns(&sweepTs); err != nil {
		t.Fatal(err)
	}
	if !sweepTs.Valid {
		t.Fatal("setup: terminal commit left no sweep_commit_ts")
	}
	return sweepTs.Time
}

// claimTerminalAdvs builds terminal advancements for freshly created
// instances, claiming every workflow task once up front: a second ClaimTasks
// would find the first call's leases still held.
func claimTerminalAdvs(t *testing.T, b *Backend, ctx context.Context, worker string, ids ...string) []backend.Advancement {
	t.Helper()
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: worker,
	})
	if err != nil {
		t.Fatal(err)
	}
	byInst := map[string]backend.Task{}
	for _, tk := range tasks {
		byInst[tk.InstanceID] = tk
	}
	var advs []backend.Advancement
	for _, id := range ids {
		task, ok := byInst[id]
		if !ok {
			t.Fatalf("setup: no claimable workflow task for %s", id)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		result := []byte(`"ok"`)
		advs = append(advs, backend.Advancement{
			InstanceID: id, TaskID: task.ID, ExpectedSeq: st.NextSeq,
			NewEvents: []journal.Event{{Seq: st.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: result}},
			Terminal:  &backend.TerminalUpdate{Status: "completed", Result: result},
		})
	}
	return advs
}

// claimTerminalAdv builds a terminal advancement for a freshly created
// instance with one claimed workflow task.
func claimTerminalAdv(t *testing.T, b *Backend, ctx context.Context, id string, worker string) backend.Advancement {
	t.Helper()
	return claimTerminalAdvs(t, b, ctx, worker, id)[0]
}

// TestCleanupTerminalInstancePreservesPostCommitSends covers the Codex
// round-21 P2 on #291: a SendToInboxBatch starting after the terminal status
// commit but before the detached sweep finishes is accepted (its dedupe
// marker plus inbox row commit), and the sweep must not delete the inbox row
// while the dedupe phase already passed — that split loses the event and
// discards every later send under the same DedupeID. Old code swept every
// row of the instance, so the post-commit ("new") rows below vanished while
// the new code preserves them and still removes the pre-terminal ("old")
// residue.
func TestCleanupTerminalInstancePreservesPostCommitSends(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	id := fmt.Sprintf("r21-sp-cutoff-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "old"}, "old"); err != nil {
		t.Fatal(err)
	}
	// The terminal commit's own sweep removes the pre-terminal signal and
	// fixes the sweep_commit_ts ordering tick on the instance row.
	if err := b.CommitAdvancement(ctx, claimTerminalAdv(t, b, ctx, id, "w1")); err != nil {
		t.Fatal(err)
	}
	sweepTs := readSweepTsForTest(t, b, ctx, id)
	oldTime := sweepTs.Add(-time.Hour)

	// Pre-terminal residue the sweep must still remove.
	if _, err := b.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap("wf_inbox", map[string]any{
			"id": newID(), "instance_id": id, "type": string(journal.TypeSignalReceived), "created_at": oldTime}),
		spanner.InsertMap("wf_signal_dedupe", map[string]any{
			"instance_id": id, "dedupe_id": "old", "created_at": oldTime}),
		spanner.InsertMap("wf_tasks", map[string]any{
			"id": newID(), "kind": "activity", "queue": "default", "instance_id": id,
			"attempt": int64(0), "visible_at": oldTime, "created_at": oldTime}),
	}); err != nil {
		t.Fatal(err)
	}
	// Racing post-commit sends the sweep must preserve (accepted after the
	// status commit: dedupe marker plus inbox row, both newer than the
	// terminal transition). They stamp the commit timestamp like
	// production sends (see commitTimestamp): an explicit future timestamp
	// is rejected outright on commit-timestamp columns, and wall-clock now
	// is this test's "after the flip".
	if _, err := b.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap("wf_inbox", map[string]any{
			"id": newID(), "instance_id": id, "type": string(journal.TypeSignalReceived), "created_at": commitTimestamp()}),
		spanner.InsertMap("wf_signal_dedupe", map[string]any{
			"instance_id": id, "dedupe_id": "new", "created_at": commitTimestamp()}),
		spanner.InsertMap("wf_tasks", map[string]any{
			"id": newID(), "kind": "activity", "queue": "default", "instance_id": id,
			"attempt": int64(0), "visible_at": nowUTC(), "created_at": commitTimestamp()}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.cleanupTerminalInstance(ctx, id, true); err != nil {
		t.Fatal(err)
	}
	// Old residue gone on every swept table...
	if n := countChildRows(t, b, ctx, "wf_signal_dedupe", id); n != 1 {
		t.Errorf("dedupe rows = %d, want 1 (old swept, racing new preserved)", n)
	}
	if n := countChildRows(t, b, ctx, "wf_inbox", id); n != 1 {
		t.Errorf("inbox rows = %d, want 1 (old swept, racing new preserved)", n)
	}
	if n := countChildRows(t, b, ctx, "wf_tasks", id); n != 1 {
		t.Errorf("task rows = %d, want 1 (old swept, racing new preserved)", n)
	}
	// ...and the survivors are exactly the post-commit rows.
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	var dedupeID string
	if err := row.Columns(&dedupeID); err != nil {
		t.Fatal(err)
	}
	iter.Stop()
	if dedupeID != "new" {
		t.Errorf("surviving dedupe guard = %q, want the post-commit %q (the sweep deleted the accepted send)", dedupeID, "new")
	}
}

// TestCommitAdvancementsContinuesPastCleanupFailure covers the Codex
// round-21 P2 on #291: a cleanup-retries-exhausted failure for one victim of
// a multi-instance batch must not skip later instances' sweeps. The batch
// commits [inst-b (doomed cleanup), inst-a (healthy)], both terminal with
// residue, against a fault-injected cleanup that fails only for inst-b. Old
// code returned B's error immediately: inst-a's residue was never swept
// (unrecoverable once the advancement committed). The new code retains the
// first error, still sweeps every committed victim, and returns it at the
// end, mirroring the DynamoDB/Firestore continuation fix from round-19.
func TestCommitAdvancementsContinuesPastCleanupFailure(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	instA := "r21-sp-cont-a-" + suffix
	instB := "r21-sp-cont-b-" + suffix
	for _, id := range []string{instA, instB} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
		// Residue beyond the in-commit mutation budget
		// (terminalCleanupMutationBudget): the commit-time sweep takes the
		// first page and only the post-commit sweep removes the rest, so a
		// failed post-commit cleanup leaves observable residue. Small
		// residue would vanish inside the commit and the test could not
		// distinguish the old early return from the fix.
		var muts []*spanner.Mutation
		for i := 0; i < terminalCleanupMutationBudget+100; i++ {
			muts = append(muts, spanner.InsertMap("wf_inbox", map[string]any{
				"id": newID(), "instance_id": id, "type": string(journal.TypeSignalReceived), "created_at": time.Now().UTC()}))
		}
		if _, err := b.client.Apply(ctx, muts); err != nil {
			t.Fatal(err)
		}
	}
	oldCleanup := terminalCleanupFunc
	terminalCleanupFunc = func(b *Backend, ctx context.Context, id string, includeInbox bool) error {
		if id == instB {
			return fmt.Errorf("boom: terminal sweep failed for %s", id)
		}
		return oldCleanup(b, ctx, id, includeInbox)
	}
	defer func() { terminalCleanupFunc = oldCleanup }()

	// Failing victim first: the old early return never reached inst-a.
	advs := claimTerminalAdvs(t, b, ctx, "w", instB, instA)
	err := b.CommitAdvancements(ctx, advs)
	if err == nil {
		t.Fatal("CommitAdvancements returned nil, want the retained inst-b cleanup error")
	}
	if !strings.Contains(err.Error(), instB) {
		t.Fatalf("returned error %v does not identify the failed victim %q", err, instB)
	}
	// The healthy victim was still swept despite the earlier failure...
	if n := countChildRows(t, b, ctx, "wf_inbox", instA); n != 0 {
		t.Errorf("inst-a inbox rows = %d, want 0 (later victim still swept)", n)
	}
	if n := countChildRows(t, b, ctx, "wf_signal_dedupe", instA); n != 0 {
		t.Errorf("inst-a dedupe rows = %d, want 0 (later victim still swept)", n)
	}
	// ...while the doomed victim keeps its residue and reports the error.
	if n := countChildRows(t, b, ctx, "wf_inbox", instB); n == 0 {
		t.Error("inst-b inbox residue missing: the failed sweep must not silently drop rows")
	}
}
