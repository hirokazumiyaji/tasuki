package tasuki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_Round9_ReleaseClaimMatchesGeneration is a regression test
// for round-9 P2a: after a restart following a shutdown timeout, track()
// replaces the old turn's entry with the new attempt under the same task
// ID. The old turn's late release must match the claim generation
// (worker + attempt, as ownsWorkflowCommit does) before deleting — a
// mismatch leaves the new entry alone. Without the match the old turn
// deletes the NEW entry, ownsWorkflowCommit fails for the live turn, and
// its lease goes untracked until expiry (and the stale release that
// follows clears the live lease on backends that match by ID alone).
func TestWorker_Round9_ReleaseClaimMatchesGeneration(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	old := newRound9Task(9, "w1", 1)
	w.track(old)
	// Restart: the new generation claims the same task ID with a new
	// attempt, replacing the old turn's entry.
	newer := newRound9Task(9, "w1", 2)
	w.track(newer)

	// The old turn's late release must be refused AND must not disturb
	// the new entry.
	if w.claimWorkflowRelease(old) {
		t.Fatal("claimWorkflowRelease(old) = true, want false (generation moved on to attempt 2)")
	}
	if !w.ownsWorkflowCommit(newer) {
		t.Fatal("ownsWorkflowCommit(new) = false after the stale release, want true (new entry must survive)")
	}
	// The live turn still owns its lease and may release it.
	if !w.claimWorkflowRelease(newer) {
		t.Fatal("claimWorkflowRelease(new) = false, want true (live turn owns the lease)")
	}
	if w.ownsWorkflowCommit(old) {
		t.Fatal("ownsWorkflowCommit(old) = true after the live release, want false (entry removed once)")
	}
}

// TestWorker_Round9_ReleaseClaimMatchesWorker is the worker-identity half
// of the generation match: a different worker's entry under the same task
// ID must also survive a stale release claim.
func TestWorker_Round9_ReleaseClaimMatchesWorker(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	mine := newRound9Task(10, "w1", 1)
	peers := newRound9Task(10, "peer", 1)
	w.track(mine)
	w.track(peers) // peer re-claimed the same task ID
	if w.claimWorkflowRelease(mine) {
		t.Fatal("claimWorkflowRelease(mine) = true, want false (entry belongs to peer now)")
	}
	if !w.ownsWorkflowCommit(peers) {
		t.Fatal("ownsWorkflowCommit(peer) = false after the stale release, want true")
	}
}

func newRound9Task(id int64, worker string, attempt int) backend.Task {
	return backend.Task{ID: id, InstanceID: "i9", WorkerID: worker, Attempt: attempt}
}

// TestWorker_Round9_WorkerCancelWinsOverOnTimeResult is a regression test
// for round-9 P2b: with LocalActivityTimeout plus shutdown, runCtx is
// canceled while the activity observes the cancel but still returns a
// value before the deadline. The on-time timestamp must not win —
// accepting it keeps the workflow executing (with side effects) until
// wf.fn returns, even though the advancement is abandoned anyway. The
// turn context error surfaces instead so the turn is abandoned promptly.
func TestWorker_Round9_WorkerCancelWinsOverOnTimeResult(t *testing.T) {
	const timeout = 50 * time.Millisecond
	produced := time.Now()
	live, liveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer liveCancel()
	runCtx, stop := context.WithCancel(context.Background())
	stop() // Shutdown/parent cancel landed before consumption

	// On-time success under a canceled turn: cancellation wins.
	if out, err := acceptLocalResult("x",
		callResult{out: []byte("v"), completed: produced}, live, runCtx, timeout); !errors.Is(err, context.Canceled) || out != nil {
		t.Fatalf("on-time success under cancel: out=%q err=%v, want (nil, turn cancellation)", out, err)
	}

	// On-time activity error under a canceled turn: cancellation wins
	// over the payload too — the turn is abandoned either way.
	actErr := errors.New("boom")
	if _, err := acceptLocalResult("e",
		callResult{err: actErr, completed: produced}, live, runCtx, timeout); !errors.Is(err, context.Canceled) {
		t.Fatalf("on-time error under cancel: err=%v, want turn cancellation", err)
	}

	// Live turn is unaffected: on-time results still pass through (no
	// regression on the production-time arbitration).
	if out, err := acceptLocalResult("ok",
		callResult{out: []byte("v"), completed: produced}, live, context.Background(), timeout); err != nil || string(out) != "v" {
		t.Fatalf("live on-time success: out=%q err=%v, want preserved", out, err)
	}

	// A nil turn context (defensive) never panics and preserves the
	// timestamp arbitration.
	if out, err := acceptLocalResult("nil",
		callResult{out: []byte("v"), completed: produced}, live, nil, timeout); err != nil || string(out) != "v" {
		t.Fatalf("nil runCtx: out=%q err=%v, want preserved", out, err)
	}
}
