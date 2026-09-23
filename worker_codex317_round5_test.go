package tasuki

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// joinOrderBackend records the completion order of ExtendLease vs
// ReleaseLease. Once armed, ExtendLease blocks on extendGate so the test can
// hold a detached renewal in flight across the handler's release.
type joinOrderBackend struct {
	backend.Backend
	mu            sync.Mutex
	events        []string
	armBlock      atomic.Bool
	extendEntered chan struct{}
	extendGate    chan struct{}
	enterOnce     atomic.Bool
}

func (b *joinOrderBackend) record(ev string) {
	b.mu.Lock()
	b.events = append(b.events, ev)
	b.mu.Unlock()
}

func (b *joinOrderBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	if b.armBlock.Load() {
		b.record("extend-enter")
		if b.enterOnce.CompareAndSwap(false, true) {
			close(b.extendEntered)
		}
		<-b.extendGate
		err := b.Backend.ExtendLease(ctx, taskID, d)
		b.record("extend-exit")
		return err
	}
	return b.Backend.ExtendLease(ctx, taskID, d)
}

func (b *joinOrderBackend) ReleaseLease(ctx context.Context, taskID int64) error {
	b.record("release-enter")
	err := b.Backend.ReleaseLease(ctx, taskID)
	b.record("release-exit")
	return err
}

func (b *joinOrderBackend) hasEvent(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ev := range b.events {
		if ev == name {
			return true
		}
	}
	return false
}

func (b *joinOrderBackend) indexOf(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, ev := range b.events {
		if ev == name {
			return i
		}
	}
	return -1
}

func (b *joinOrderBackend) snapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...)
}

// releaseJoinCtx is a hook context that freezes handleActivity at the
// post-invocation cancel check. The first Err call after the activity
// function has returned blocks until the test releases it; once released,
// Err reports context.Canceled so the handler takes the shutdown-release
// path. Done is test-controlled.
type releaseJoinCtx struct {
	context.Context
	returned   *atomic.Bool
	doneCh     chan struct{}
	errEntered chan struct{}
	errRelease chan struct{}
	errOnce    atomic.Bool
}

func (c *releaseJoinCtx) Done() <-chan struct{} { return c.doneCh }

func (c *releaseJoinCtx) Err() error {
	if c.returned.Load() {
		if c.errOnce.CompareAndSwap(false, true) {
			close(c.errEntered)
			<-c.errRelease
		}
		return context.Canceled
	}
	return nil
}

// TestWorker_CanceledActivityReleaseJoinsRenewal covers the release-path /
// renewal race: a periodic ExtendLease still in flight when a canceled
// activity return takes the shutdown-release path must complete before the
// ReleaseLease lands. A renewal landing after the release re-hides the
// task for a full lease (or modifies a peer's fresh lease — backend lease
// ops are keyed by task ID alone).
//
// Detached-commit renewal cannot be in flight on this path: detached mode
// is entered atomically with the commit ownership transfer (see
// beginDetachedCommit), and the release path never transfers, so the flag
// is still clear and the loop cannot enter detached mode after the cancel.
// The renewal joined here is therefore an ordinary periodic tick renewal,
// held in a gated ExtendLease. The hook context freezes the handler at the
// post-invocation cancel check with that renewal already in flight, so the
// interleaving is deterministic: with the fix the release path joins the
// renewal loop before releasing — no release is observable while the
// renewal is blocked, extend-exit precedes release-exit, and the released
// task stays claimable. Without the join the release lands while the
// renewal is still in flight and the renewal lands last, re-hiding the
// task.
func TestWorker_CanceledActivityReleaseJoinsRenewal(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &joinOrderBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendGate:    make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		// 500ms tick: a periodic renewal is guaranteed in flight during
		// the test, while the 1s lease keeps the entry locally valid
		// through the release.
		LeaseDuration:          time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "release-join-1", "hooked")

	var returned atomic.Bool
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		returned.Store(true)
		return "ok", nil
	}, WithName("hooked"))

	hook := &releaseJoinCtx{
		Context:    context.Background(),
		returned:   &returned,
		doneCh:     make(chan struct{}),
		errEntered: make(chan struct{}),
		errRelease: make(chan struct{}),
	}
	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(hook, task, tok) }()

	select {
	case <-hook.errEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach the post-invocation cancel check")
	}
	// Gate renewals and wait for a periodic tick renewal to get in
	// flight while the handler is still frozen at the check.
	store.armBlock.Store(true)
	select {
	case <-store.extendEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("periodic renewal did not start")
	}
	close(hook.doneCh) // cancel lands while the handler is frozen at the check
	time.Sleep(50 * time.Millisecond)
	close(hook.errRelease) // the check reports cancel; the handler takes the release path

	// The release path must join the in-flight renewal before releasing:
	// while the renewal is still blocked, no release may be observable.
	time.Sleep(200 * time.Millisecond)
	if store.hasEvent("release-enter") {
		// Without the join the release already landed. Drain so the test
		// does not leak goroutines, then fail.
		close(store.extendGate)
		<-herrCh
		t.Fatal("release ran while a renewal was still in flight (release path must join renewal termination first)")
	}
	close(store.extendGate)
	select {
	case herr := <-herrCh:
		if herr == nil {
			t.Fatal("handleActivity = nil, want cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the renewal was released")
	}
	if a, b := store.indexOf("extend-exit"), store.indexOf("release-exit"); a < 0 || b < 0 || !(a < b) {
		t.Fatalf("lease op order = %v, want extend-exit before release-exit", store.snapshot())
	}
	// The release must stand instead of being overwritten by a late
	// renewal: the task is claimable by a peer.
	if peer := probeActivityTasks(t, ctx, mem); len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1 (late renewal must not re-hide the released task)", len(peer))
	}
}
