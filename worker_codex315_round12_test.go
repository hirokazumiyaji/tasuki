package tasuki

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_Round12_FailedTurnUntrackFenced is the regression test for
// round-12 P2 (match the claim generation before untracking failed turns):
// after a shutdown-timeout restart the new generation tracks a new attempt
// under the same task ID while the old turn is still blocked (e.g. on the
// instance actor). When the old backend call returns a domain/store error
// (not a cancellation) the failed-turn branch must remove its entry only
// when WorkerID+Attempt still match — the same generation gate as
// claimWorkflowRelease — and leave the new entry alone on mismatch.
//
// Without the gate the old branch deletes the NEW entry by ID:
// ownsWorkflowCommit then fails for the live turn, its commit is skipped,
// and side effects may repeat post-expiry.
func TestWorker_Round12_FailedTurnUntrackFenced(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	old := backend.Task{ID: 42, Kind: "workflow", InstanceID: "i12", WorkerID: "w1", Attempt: 1}
	w.track(old)
	// Restart: the new generation claims the same task ID with a new
	// attempt, replacing the old turn's entry.
	newer := backend.Task{ID: 42, Kind: "workflow", InstanceID: "i12", WorkerID: "w1", Attempt: 2}
	w.track(newer)

	// The old turn's failed-branch cleanup must not disturb the new entry.
	w.untrackWorkflow(old)
	if !w.ownsWorkflowCommit(newer) {
		t.Fatal("ownsWorkflowCommit(new) = false after the stale failed-turn untrack, want true (new entry must survive)")
	}
	if w.ownsWorkflowCommit(old) {
		t.Fatal("ownsWorkflowCommit(old) = true after the stale untrack, want false")
	}
	// The live turn still owns its lease; its own cleanup removes it once.
	w.untrackWorkflow(newer)
	if w.ownsWorkflowCommit(newer) {
		t.Fatal("ownsWorkflowCommit(new) = true after the live untrack, want false (entry removed once)")
	}
}

// TestWorker_Round12_FailedTurnUntrackMatchesWorker is the worker-identity
// half of the generation match: a peer's entry under the same task ID must
// also survive a stale failed-turn untrack.
func TestWorker_Round12_FailedTurnUntrackMatchesWorker(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	mine := backend.Task{ID: 43, Kind: "workflow", InstanceID: "i12", WorkerID: "w1", Attempt: 1}
	peers := backend.Task{ID: 43, Kind: "workflow", InstanceID: "i12", WorkerID: "peer", Attempt: 1}
	w.track(mine)
	w.track(peers) // peer re-claimed the same task ID
	w.untrackWorkflow(mine)
	if !w.ownsWorkflowCommit(peers) {
		t.Fatal("ownsWorkflowCommit(peer) = false after the stale untrack, want true")
	}
}

// TestWorker_Round12_FailedPendingUntrackFenced covers the same gate on the
// flush-disposal path: a pending commit that lost ownership before the
// flush (skipped, returned in the failed subset) is disposed with a live
// tick context via untrack — the stale pending's task must not delete its
// successor's entry.
func TestWorker_Round12_FailedPendingUntrackFenced(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	old := backend.Task{ID: 44, Kind: "workflow", InstanceID: "i12", WorkerID: "w1", Attempt: 1}
	w.track(old)
	newer := backend.Task{ID: 44, Kind: "workflow", InstanceID: "i12", WorkerID: "w1", Attempt: 2}
	w.track(newer)

	stale := pendingWorkflowCommit{
		instanceID: "i12",
		adv:        backend.Advancement{InstanceID: "i12", TaskID: 44},
		task:       old,
	}
	w.untrackPending(stale)
	if !w.ownsWorkflowCommit(newer) {
		t.Fatal("ownsWorkflowCommit(new) = false after the stale pending untrack, want true (new entry must survive)")
	}
}
