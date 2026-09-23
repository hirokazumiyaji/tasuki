package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// blockReleaseBackend blocks the first ReleaseLease call on a gate so a test
// can hold Shutdown inside releaseInFlight while attempting a concurrent
// restart.
type blockReleaseBackend struct {
	backend.Backend
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *blockReleaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_Round12_ShutdownRestartFenced is the regression test for
// round-12 P1 (bind shutdown cancellation to the stopped generation): a
// StartWithError racing Shutdown — after Shutdown cleared cancel/done but
// before its actMu capture — must not install a new execCancel that the old
// Shutdown then captures and cancels at grace expiry (leaving the old
// execCtx live and old activities running post-lease-release).
//
// With the fix Shutdown sets a shuttingDown flag under mu at entry (cleared
// at return) and StartWithError rejects restarts with ErrWorkerShuttingDown
// while it is set. The test holds Shutdown inside releaseInFlight, attempts
// a restart, and requires the rejection; after Shutdown returns a restart
// must succeed with a live execution context.
//
// Without the fix the concurrent StartWithError returns nil (it only checks
// cancel != nil, already cleared), installs a new generation, and the old
// Shutdown cancels it — the restart assertion fails.
func TestWorker_Round12_ShutdownRestartFenced(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &blockReleaseBackend{Backend: mem, entered: make(chan struct{}), release: make(chan struct{})}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	if err := w.StartWithError(ctx); err != nil {
		t.Fatalf("first StartWithError: %v", err)
	}
	// Keep one tracked lease so Shutdown blocks in releaseInFlight on the
	// gated ReleaseLease.
	tok := w.track(999)
	defer w.untrack(999, tok)
	oldExec := w.execContext(ctx)

	shutDone := make(chan error, 1)
	go func() { shutDone <- w.Shutdown(context.Background()) }()

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not enter releaseInFlight")
	}
	// Shutdown cleared Running at entry; the restart below races its grace.
	deadline := time.Now().Add(3 * time.Second)
	for w.Running() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if w.Running() {
		close(store.release)
		<-shutDone
		t.Fatal("worker still Running after Shutdown entry (release gate entered but Running not cleared)")
	}

	if err := w.StartWithError(ctx); !errors.Is(err, ErrWorkerShuttingDown) {
		close(store.release)
		<-shutDone
		t.Fatalf("concurrent StartWithError during Shutdown = %v, want ErrWorkerShuttingDown (restart must wait for Shutdown)", err)
	}

	close(store.release)
	select {
	case err := <-shutDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the release gate opened")
	}
	// The stopped generation was fenced: its execution context is canceled
	// at grace expiry and never leaks live past the lease release.
	select {
	case <-oldExec.Done():
	default:
		t.Fatal("old execCtx still live after Shutdown (stopped generation was not canceled)")
	}

	// After Shutdown returns a restart must succeed with a live execution
	// context (the old generation was fenced, not leaked onto the new one).
	if err := w.StartWithError(ctx); err != nil {
		t.Fatalf("restart StartWithError after Shutdown: %v", err)
	}
	if !w.Running() {
		t.Fatal("Running = false after restart, want true")
	}
	execCtx := w.execContext(ctx)
	select {
	case <-execCtx.Done():
		t.Fatal("restarted execCtx already canceled (old Shutdown canceled the new generation)")
	default:
	}
	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("final Shutdown: %v", err)
	}
}

// TestWorker_Round12_LeaseDeadlineMonotonic is the regression test for
// round-12 P2 (keep local lease deadlines monotonic): heartbeat and periodic
// renewal overlap, both stamp their start pre-call, and out-of-order returns
// must not let the older start overwrite the newer deadline. Without the fix
// the second (older) refreshLeaseAt regresses the expiry, ownsFresh stops
// early, and the detached guard can reject a valid result while the backend
// lease is still live.
func TestWorker_Round12_LeaseDeadlineMonotonic(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: 10 * time.Second,
		WorkerID:      "w1",
	})
	base := time.Now().UTC()
	tok := w.trackAt(7, base)
	defer w.untrack(7, tok)

	newer := base.Add(500 * time.Millisecond)
	w.refreshLeaseAt(7, tok, newer)
	w.mu.Lock()
	expiry := w.inFlight[7].expiry
	w.mu.Unlock()
	if !expiry.Equal(newer.Add(10 * time.Second)) {
		t.Fatalf("after newer refresh expiry=%v, want %v", expiry, newer.Add(10*time.Second))
	}

	// The older renewal returns late: it must not regress the deadline.
	w.refreshLeaseAt(7, tok, base)
	w.mu.Lock()
	expiry = w.inFlight[7].expiry
	w.mu.Unlock()
	if !expiry.Equal(newer.Add(10 * time.Second)) {
		t.Fatalf("out-of-order refresh regressed expiry to %v, want max %v", expiry, newer.Add(10*time.Second))
	}

	// A genuinely later renewal must still advance the deadline.
	later := newer.Add(time.Second)
	w.refreshLeaseAt(7, tok, later)
	w.mu.Lock()
	expiry = w.inFlight[7].expiry
	w.mu.Unlock()
	if !expiry.Equal(later.Add(10 * time.Second)) {
		t.Fatalf("later refresh expiry=%v, want %v", expiry, later.Add(10*time.Second))
	}
}
