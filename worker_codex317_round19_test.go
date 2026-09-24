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

// cancelStallExtendBackend blocks the FIRST ExtendLease unconditionally,
// ignoring context cancellation like a context-ignoring backend. Later
// calls delegate directly so the detached release still succeeds. It
// counts landed writes so a test can prove the handler returned while
// the renewal was still blocked.
type cancelStallExtendBackend struct {
	backend.Backend
	mu        sync.Mutex
	calls     int
	landed    int
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *cancelStallExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	first := b.calls == 1
	b.mu.Unlock()
	if !first {
		return b.Backend.ExtendLease(ctx, taskID, d)
	}
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	// Deliberately deaf to ctx: the stuck ordinary renewal stays blocked
	// across grace expiry even though the execution context is canceled.
	<-b.release
	err := b.Backend.ExtendLease(context.Background(), taskID, d)
	b.mu.Lock()
	b.landed++
	b.mu.Unlock()
	return err
}

func (b *cancelStallExtendBackend) landedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.landed
}

// TestWorker_Round19_CancelPathBoundedOnStuckOrdinaryRenewal is the
// regression test for round-19 P1 (bound the cancellation-path renewal
// join). Grace expiry with an ordinary ExtendLease blocked in a
// context-ignoring backend used to hold the handler forever: the
// context-aware activity returns, the cancellation branch waited
// unconditionally on renewDone, and — with the bounded joinCommitStop
// teardown not yet installed on that path — the handler never
// actWg.Done'd nor released its activity semaphore slot, so with
// ActivityConcurrency==1 a restarted worker could not execute
// activities. The same held for exitDetachedCommit.
//
// Layout: handleActivity runs with its inherited renewal loop, the
// loop's ordinary ticker renewal blocks in the backend, the execution
// context is canceled (grace expiry), the context-aware activity
// returns promptly, and the handler must still return within
// commitJoinCap — while the renewal is still blocked — releasing its
// slot. Without the bound the handler never returns. The late landing
// afterwards refreshes nothing (entry released, guard dropped).
func TestWorker_Round19_CancelPathBoundedOnStuckOrdinaryRenewal(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &cancelStallExtendBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          2 * time.Second,
		WorkerID:               "w1",
		ActivityConcurrency:    1,
		IncompatibleRetryDelay: -1,
	})
	// Context-aware activity: returns promptly once the execution
	// context is canceled, so the handler reaches the cancellation
	// branch while the ordinary renewal is still blocked.
	RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-store.entered:
		case <-time.After(10 * time.Second):
			return "", errors.New("ordinary renewal never entered ExtendLease")
		}
		select {
		case <-actCtx.Done():
			return "", actCtx.Err()
		case <-time.After(10 * time.Second):
			return "", errors.New("execution context never canceled")
		}
	}, WithName("hooked"))
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round19-cancel-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	// Model the tickActivities wrapper holding the single activity slot
	// and the actWg entry: both must be released when the handler
	// returns, or a restarted worker cannot execute.
	w.actSem <- struct{}{}
	done, ok := w.trackActivity()
	if !ok {
		t.Fatal("trackActivity refused while the worker is running")
	}

	execCtx, cancel := context.WithCancel(context.Background())
	herrCh := make(chan error, 1)
	go func() {
		defer done()
		defer func() { <-w.actSem }()
		herrCh <- w.handleActivity(execCtx, task, tok)
	}()

	// Wait until the ordinary renewal is genuinely blocked, then expire
	// the grace: the activity returns and the handler must join
	// boundedly instead of forever.
	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("ordinary renewal never entered ExtendLease")
	}
	cancelTime := time.Now()
	cancel()

	select {
	case herr := <-herrCh:
		if !errors.Is(herr, context.Canceled) {
			t.Fatalf("handleActivity = %v, want context.Canceled (grace-expired cancellation must surface)", herr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("handleActivity did not return within 15s after grace expiry while an ordinary renewal stayed blocked (cancellation-path join must be bounded by commitJoinCap)")
	}
	elapsed := time.Since(cancelTime)
	// The bounded join must actually engage — not skip the wait: an
	// immediate return would release the slot without joining a
	// renewal that settles within the cap. The floor sits far from
	// both (~0s skipped vs ~5s capped) so CI jitter cannot flip it.
	if elapsed < 3*time.Second {
		t.Fatalf("handleActivity returned in %v after cancel, want >=3s (cancellation-path join must wait — boundedly, not skip — for the stuck renewal)", elapsed)
	}
	if n := store.landedCount(); n != 0 {
		t.Fatalf("stuck renewal landed %d times before the handler returned, want 0 (cancellation path must give up, not join, the blocked renewal)", n)
	}

	// The slot must be freed: with ActivityConcurrency==1 a second
	// activity must be able to acquire it promptly after the return.
	select {
	case w.actSem <- struct{}{}:
		<-w.actSem
	default:
		t.Fatal("activity semaphore slot still held after the handler returned (restarted worker could not execute activities)")
	}

	// Release the stuck renewal: it lands after the release against a
	// released entry and a dropped guard, so it must refresh nothing.
	close(store.release)
	deadline := time.Now().Add(10 * time.Second)
	for store.landedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if store.landedCount() == 0 {
		t.Fatal("stuck renewal never landed after the gate release")
	}
	time.Sleep(200 * time.Millisecond)
	w.detMu.Lock()
	_, stillGuarded := w.detGuard[task.ID]
	w.detMu.Unlock()
	if stillGuarded {
		t.Fatal("detached guard still present after the cancellation release and the late renewal landing (must be dropped)")
	}
	w.mu.Lock()
	_, stillTracked := w.inFlight[task.ID]
	w.mu.Unlock()
	if stillTracked {
		t.Fatal("task still tracked after the cancellation release (must be released for a peer retry)")
	}
}
