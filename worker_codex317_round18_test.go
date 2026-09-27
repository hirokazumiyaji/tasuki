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

// ignoreCtxStallExtendBackend blocks the FIRST ExtendLease unconditionally:
// it never observes context cancellation, reproducing a renewal stuck in a
// context-ignoring backend (memory ignores cancellation the same way; a
// wedged store blocks the same way). Later calls delegate directly so the
// synchronous pre-commit detached renewal still succeeds. It counts backend
// entries and landed writes so a test can prove a commit tore down while
// the renewal was still blocked, and that the late landing was harmless.
type ignoreCtxStallExtendBackend struct {
	backend.Backend
	mu        sync.Mutex
	calls     int
	landed    int
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *ignoreCtxStallExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	first := b.calls == 1
	b.mu.Unlock()
	if !first {
		return b.Backend.ExtendLease(ctx, task, d)
	}
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	// Deliberately deaf to ctx: the stuck ordinary renewal from the
	// finding stays blocked across the pre-commit abort and the commit.
	<-b.release
	err := b.Backend.ExtendLease(context.Background(), task, d)
	b.mu.Lock()
	b.landed++
	b.mu.Unlock()
	return err
}

func (b *ignoreCtxStallExtendBackend) landedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.landed
}

// TestWorker_Round18_DeferredTeardownBoundedOnStuckOrdinaryRenewal is the
// regression test for round-18 P1 (bound the deferred renewal join after
// aborting the commit), driven through the real deferred-teardown path in
// handleActivity rather than the join helper directly.
//
// It covers two defects at once. First, every production caller wrote
// `defer joinCommitStop(...)`, which only defers BUILDING the teardown
// closure and discards it at return — the renewal loop was never joined
// (round-18 P1a); the deferred teardown must be invoked as
// `defer joinCommitStop(...)()`. The round-10 test missed this because it
// invokes the closure directly. Second, once the teardown actually runs,
// its renewDone wait must be bounded (round-18 P1b): the round-17 cap
// aborts a row-preserving commit when a same-task ordinary renewal stays
// stuck, but an unconditional post-commit wait would hold the activity
// slot forever on a context-ignoring backend despite the cap.
//
// Layout: handleActivity runs with its inherited renewal loop, the
// loop's ordinary ticker renewal blocks in the backend, the activity
// completes, the row-deleting commit succeeds (it joins no ordinary
// renewals), and the handler must still return within commitJoinCap —
// while the renewal is still blocked. Without the invocation fix the
// handler returns immediately without joining (the renewal is still
// blocked at return, but nothing waited); without the bound it never
// returns. Either way the stuck renewal landing afterwards, against a
// transferred-out entry and a dropped guard, refreshes nothing and the
// completion stands.
func TestWorker_Round18_DeferredTeardownBoundedOnStuckOrdinaryRenewal(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &ignoreCtxStallExtendBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          2 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	// The activity returns only once the inherited loop's ordinary
	// renewal is stuck in the backend, so the commit and its deferred
	// teardown race a genuinely blocked renewal.
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		select {
		case <-store.entered:
			return "ok", nil
		case <-time.After(10 * time.Second):
			return "", errors.New("ordinary renewal never entered ExtendLease")
		}
	}, WithName("hooked"))
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round18-teardown-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	herrCh := make(chan error, 1)
	start := time.Now()
	go func() { herrCh <- w.handleActivity(ctx, task, tok) }()

	// The row-deleting commit needs no ordinary-renewal join, so the
	// deferred teardown is the only thing that can hold the handler
	// past the ~1s renewal tick: with the fix it gives up within
	// commitJoinCap even though the renewal is still blocked (release
	// stays open). Without the bound the handler never returns.
	select {
	case herr := <-herrCh:
		if herr != nil {
			t.Fatalf("handleActivity = %v, want nil (row-deleting commit must succeed; only the teardown races the stuck renewal)", herr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("handleActivity did not return within 15s while an ordinary renewal stayed blocked (deferred teardown must be bounded by commitJoinCap)")
	}
	elapsed := time.Since(start)
	// The teardown must actually engage the bound — not skip the join:
	// without the invocation fix (`defer joinCommitStop(...)` discarding
	// the closure) the handler returns ~1s after start having waited on
	// nothing; with the fix it returns only after the ~5s cap gives up
	// on the still-blocked renewal. The floor sits far from both (~1s
	// vs ~6s) so ordinary CI jitter cannot flip it.
	if elapsed < 3*time.Second {
		t.Fatalf("handleActivity returned in %v, want >=3s (deferred teardown must join — boundedly, not skip — the stuck renewal)", elapsed)
	}
	if n := store.landedCount(); n != 0 {
		t.Fatalf("stuck renewal landed %d times before the handler returned, want 0 (teardown must give up, not join, the blocked renewal)", n)
	}

	// Release the stuck renewal: it lands after the commit against a
	// transferred-out entry and a dropped guard, so it must refresh
	// nothing and leave the completion standing.
	close(store.release)
	deadline := time.Now().Add(10 * time.Second)
	for store.landedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if store.landedCount() == 0 {
		t.Fatal("stuck renewal never landed after the gate release")
	}
	// Let the loop observe done and exit.
	time.Sleep(200 * time.Millisecond)
	w.detMu.Lock()
	_, stillGuarded := w.detGuard[task.ID]
	w.detMu.Unlock()
	if stillGuarded {
		t.Fatal("detached guard still present after the commit and the late renewal landing (must be dropped)")
	}

	// The completion must stand and nothing may be reclaimable: the
	// late renewal must not have resurrected or overwritten the task.
	mem.SetNow(t0.Add(30 * time.Second))
	if probe := probeActivityTasks(t, ctx, mem); len(probe) != 0 {
		t.Fatalf("peer claimed %d tasks, want 0 (late renewal must not resurrect the completed task)", len(probe))
	}
}
