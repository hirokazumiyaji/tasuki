package tasuki

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// orderExtendBackend blocks the first ExtendLease on a test-controlled gate
// so two overlapping detached renewals complete out of order: the
// earlier-started call returns last.
type orderExtendBackend struct {
	backend.Backend
	mu        sync.Mutex
	calls     int
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *orderExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	n := b.calls
	b.mu.Unlock()
	if n == 1 {
		if b.enterOnce.CompareAndSwap(false, true) {
			close(b.entered)
		}
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
			return errors.New("test stalled: extend release never closed")
		}
	}
	return nil
}

// TestWorker_Round13_DetachedGuardDeadlineMonotonic is the regression test
// for round-13 P2 (keep detached guard deadlines monotonic): the sync
// pre-commit renewal overlaps the inherited renewal loop, both stamp their
// start pre-call, and out-of-order completion must not let the
// earlier-started call overwrite the newer deadline. Without the fix the
// late older return regresses the guard deadline — the guard then expires
// while the backend lease is still live, the periodic renewal cancels a
// valid completion, and the retry duplicates side effects.
func TestWorker_Round13_DetachedGuardDeadlineMonotonic(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &orderExtendBackend{Backend: mem, entered: make(chan struct{}), release: make(chan struct{})}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 10 * time.Second,
		WorkerID:      "w1",
	})

	const taskID = int64(999)
	tok := w.track(taskID)
	var committing atomic.Bool
	if !w.beginDetachedCommit(taskID, tok, &committing) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	defer w.dropDetachedGuard(taskID, tok)

	// The older renewal starts first and blocks inside ExtendLease.
	olderCh := make(chan error, 1)
	go func() { olderCh <- w.renewOnceDetached(ctx, taskID, tok) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("older renewal never entered ExtendLease")
	}
	// Separate the pre-call stamps so the newer deadline is strictly
	// later than the older one.
	time.Sleep(150 * time.Millisecond)

	// The newer renewal completes first and advances the deadline.
	if err := w.renewOnceDetached(ctx, taskID, tok); err != nil {
		close(store.release)
		t.Fatalf("newer renewOnceDetached = %v, want nil", err)
	}
	w.detMu.Lock()
	newer, ok := w.detGuard[taskID]
	w.detMu.Unlock()
	if !ok {
		close(store.release)
		t.Fatal("detached guard missing after the newer renewal")
	}

	// The older renewal returns late: it must not regress the deadline.
	close(store.release)
	select {
	case err := <-olderCh:
		if err != nil {
			t.Fatalf("older renewOnceDetached = %v, want nil (both renewals cover a live lease)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("older renewal did not return after the gate opened")
	}
	w.detMu.Lock()
	final, ok := w.detGuard[taskID]
	w.detMu.Unlock()
	if !ok {
		t.Fatal("detached guard missing after the older renewal")
	}
	if !final.deadline.Equal(newer.deadline) {
		t.Fatalf("out-of-order renewal moved guard deadline to %v, want max %v (older start must not overwrite the newer deadline)",
			final.deadline, newer.deadline)
	}
}
