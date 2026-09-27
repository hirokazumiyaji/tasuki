package firestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestSweepKeepsRow pins the terminal-sweep cutoff predicate (Codex
// round-21 P2 on #291): a candidate survives only when its server update
// time postdates the terminal flip's; an unknown flip (zero time, e.g. the
// instance doc is gone) sweeps everything, matching the pre-cutoff behavior.
func TestSweepKeepsRow(t *testing.T) {
	flip := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		rowUpdate time.Time
		flip      time.Time
		keep      bool
	}{
		{"pre-flip row sweeps", flip.Add(-time.Hour), flip, false},
		{"same-tick row sweeps", flip, flip, false},
		{"post-flip racing send survives", flip.Add(time.Second), flip, true},
		{"unknown flip sweeps all", flip.Add(time.Second), time.Time{}, false},
	}
	for _, c := range cases {
		if got := sweepKeepsRow(c.rowUpdate, c.flip); got != c.keep {
			t.Errorf("%s: sweepKeepsRow = %v, want %v", c.name, got, c.keep)
		}
	}
}

// TestCleanupTerminalDocsPreservesPostCommitSends covers the Codex round-21
// P2 on #291: a SendToInboxBatch starting after the terminal status commit
// but before the detached sweep finishes is accepted (its dedupe marker plus
// inbox row commit), and the sweep must not delete the inbox row while the
// dedupe phase already passed — that split loses the event and discards
// every later send under the same DedupeID. Old code swept every row with
// instance_id == id, so the post-commit ("new") rows below vanished while
// the new code preserves them and still removes the pre-terminal ("old")
// residue (the old row rides the public API through the terminal commit's
// own sweep).
//
// The cutoff compares server commit order (document update times), not
// client entry times: TerminateInstance stamps completed_at at API entry
// while the flip serializes later, so a racing CompleteActivity that starts
// after that entry but commits before the flip carries created_at >
// completed_at while genuinely predating the transition — an entry-time
// cutoff preserves it and fails conformance TerminateCompleteRace, while the
// update-time cutoff sweeps it exactly.
func TestCleanupTerminalDocsPreservesPostCommitSends(t *testing.T) {
	ctx := context.Background()
	b := parentEnsureTestBackend(t)

	// Unique per run: the shared emulator may host concurrent suites and
	// re-runs must not collide on journal/doc IDs.
	id := fmt.Sprintf("r21-sweep-cutoff-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF"}); err != nil {
		t.Fatal(err)
	}
	// One pre-terminal signal, then the terminal commit: its own sweep must
	// remove the pre-flip residue (sweep direction).
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "old"}, "old"); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	// The shared emulator may host concurrent suites: select this
	// instance's task instead of assuming the first claim is ours.
	var task backend.Task
	for _, tk := range tasks {
		if tk.InstanceID == id {
			task = tk
		}
	}
	if task.ID == 0 {
		t.Fatalf("claim: no task for %s in %#v", id, tasks)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	result := []byte(`"ok"`)
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: task.ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{Seq: st.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: result}},
		Terminal:  &backend.TerminalUpdate{Status: "completed", Result: result},
	}); err != nil {
		t.Fatal(err)
	}
	if n := countDocs(t, b, ctx, "wf_inbox", id); n != 0 {
		t.Fatalf("terminal commit left %d inbox rows, want 0 (pre-flip residue must be swept)", n)
	}
	if n := countDocs(t, b, ctx, "wf_signal_dedupe", id); n != 0 {
		t.Fatalf("terminal commit left %d dedupe rows, want 0 (pre-flip residue must be swept)", n)
	}

	// Racing post-commit sends: accepted after the status commit (dedupe
	// marker plus inbox row, committed after the flip). A later sweep must
	// preserve them (preserve direction).
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "s"}
	newInbox := newID()
	if _, err := b.ref("wf_inbox", inboxID(id, newInbox)).Set(ctx, inboxDoc(id, newInbox, 2, ev, nowUTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ref("wf_signal_dedupe", signalDedupeID(id, "new")).Set(ctx,
		map[string]any{"instance_id": id, "dedupe_id": "new", "created_at": nowUTC()}); err != nil {
		t.Fatal(err)
	}
	newTask := actTaskID(newID())
	if _, err := b.ref("wf_tasks", newTask).Set(ctx,
		map[string]any{"id": int64(8), "kind": "activity", "queue": "default", "instance_id": id, "created_at": nowUTC()}); err != nil {
		t.Fatal(err)
	}

	if err := b.cleanupTerminalDocs(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, dd := range []struct{ col, doc string }{
		{"wf_inbox", inboxID(id, newInbox)},
		{"wf_signal_dedupe", signalDedupeID(id, "new")},
		{"wf_tasks", newTask},
	} {
		s, err := b.ref(dd.col, dd.doc).Get(ctx)
		if err != nil || !s.Exists() {
			t.Errorf("post-commit accepted send %s/%s was deleted by the sweep (dedupe/inbox split loses the signal)", dd.col, dd.doc)
		}
		_, _ = b.ref(dd.col, dd.doc).Delete(ctx)
	}
	_, _ = b.ref("wf_instances", id).Delete(ctx)
}

// countDocs counts an instance's documents in one collection.
func countDocs(t *testing.T, b *Backend, ctx context.Context, col, id string) int {
	t.Helper()
	it := b.col(col).Where("instance_id", "==", id).Documents(ctx)
	defer it.Stop()
	n := 0
	for {
		if _, err := it.Next(); err != nil {
			return n
		}
		n++
	}
}
