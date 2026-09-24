package spanner

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// seedDueTimer inserts one already-due timer row for id directly.
func seedDueTimer(t *testing.T, b *Backend, ctx context.Context, id string, seq int64) {
	t.Helper()
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_timers", map[string]any{
				"instance_id": id, "seq": seq, "fire_at": nowUTC().Add(-time.Hour),
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestFireDueTimers_DiscardsTerminalTimers covers the Codex round-23 P1
// finding on #296, mirrored from Firestore: a timer due after a
// status-only terminal commit but before the sweep deletes it must be
// discarded without recording. The race is driven deterministically — seed
// a due timer, commit the terminate status flip while the sweep is paused
// (flipStatusWithoutSweep), then fire. Without the in-transaction running
// check the timer delete stands and a TimerFired inbox row is inserted on
// the terminal instance (enqueueWorkflowTask already gates task creation
// only); the sweep later deletes that event while the test observes the
// leak. The fixed code deletes the timer and records nothing.
func TestFireDueTimers_DiscardsTerminalTimers(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "fire-after-terminate"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	seedDueTimer(t, b, ctx, id, 3)
	// Status-only terminal commit: the sweep has not run, so the due timer
	// row is still present when FireDueTimers observes it.
	flipStatusWithoutSweep(t, b, ctx, id)

	if _, err := b.FireDueTimers(ctx, 10); err != nil {
		t.Fatalf("FireDueTimers: %v", err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 0 {
		t.Fatalf("terminal timers = %d, want 0 (due timer deleted, event discarded)", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_inbox", "id", id); n != 0 {
		t.Fatalf("terminal inbox rows = %d, want 0 (no TimerFired event on a terminal instance)", n)
	}

	// Positive control: a due timer on a running instance still fires and
	// records its event.
	const live = "fire-while-running"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: live, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	seedDueTimer(t, b, ctx, live, 5)
	if _, err := b.FireDueTimers(ctx, 10); err != nil {
		t.Fatalf("FireDueTimers (running): %v", err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", live); n != 0 {
		t.Fatalf("running timers = %d, want 0 (due timer fired)", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_inbox", "id", live); n != 1 {
		t.Fatalf("running inbox rows = %d, want 1 (TimerFired event recorded)", n)
	}
}
