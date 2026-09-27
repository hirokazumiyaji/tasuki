package worker

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

// stallExtendCountingBackend blocks a selected subset of ExtendLease calls
// unconditionally, ignoring context cancellation like a context-ignoring
// backend, while counting releases. Calls not selected delegate directly.
type stallExtendCountingBackend struct {
	backend.Backend
	mu        sync.Mutex
	calls     int
	stall     func(call int) bool
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
	landed    int
	releases  atomic.Int32
}

func (b *stallExtendCountingBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	call := b.calls
	b.mu.Unlock()
	if b.stall != nil && b.stall(call) {
		if b.enterOnce.CompareAndSwap(false, true) {
			close(b.entered)
		}
		// Deliberately deaf to ctx: the stuck renewal stays blocked
		// across grace expiry even though its context is canceled.
		<-b.release
		err := b.Backend.ExtendLease(context.Background(), task, d)
		b.mu.Lock()
		b.landed++
		b.mu.Unlock()
		return err
	}
	return b.Backend.ExtendLease(ctx, task, d)
}

func (b *stallExtendCountingBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

func (b *stallExtendCountingBackend) extendCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *stallExtendCountingBackend) landedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.landed
}

// TestWorker_CancelPathSkipsReleaseOnStuckRenewal is the regression
// test for round-20 P1a (do not release while an ordinary renewal is still
// live). The round-19 bounded join gives up on a stuck ordinary ExtendLease
// and then proceeded to claimReleaseOwnership + ReleaseLease while the
// renewal was still live: with a fresh local lease the release lands first
// and the delayed renewal lands after it, re-hiding the task for a full
// lease; with an expired local lease a peer may reclaim before the delayed
// renewal lands, which then extends the peer's lease.
//
// With the fix a timed-out join suppresses the release: the guard is
// dropped and the entry is untracked WITHOUT releasing, leaving the task
// to natural expiry (the stuck renewal keeps it hidden anyway). Without
// the fix a ReleaseLease is issued while the renewal is still blocked.
//
// Layout: like the round-19 cancellation test, but the lease (15s) stays
// fresh past the bounded join (5s cap), so the old code would actually
// issue the release. The activity is context-aware and returns promptly
// on grace expiry while the ordinary renewal stays blocked.
func TestWorker_CancelPathSkipsReleaseOnStuckRenewal(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &stallExtendCountingBackend{
		Backend: mem,
		stall:   func(call int) bool { return call == 1 },
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          15 * time.Second,
		WorkerID:               "w1",
		ActivityConcurrency:    1,
		IncompatibleRetryDelay: -1,
	})
	RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-store.entered:
		case <-time.After(30 * time.Second):
			return "", errors.New("ordinary renewal never entered ExtendLease")
		}
		select {
		case <-actCtx.Done():
			return "", actCtx.Err()
		case <-time.After(30 * time.Second):
			return "", errors.New("execution context never canceled")
		}
	}, WithName("hooked"))
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round20-cancel-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

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

	select {
	case <-store.entered:
	case <-time.After(30 * time.Second):
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
	case <-time.After(20 * time.Second):
		t.Fatal("handleActivity did not return after grace expiry while an ordinary renewal stayed blocked (cancellation-path join must be bounded by commitJoinCap)")
	}
	elapsed := time.Since(cancelTime)
	// The bounded join must actually engage — not skip the wait — so the
	// floor sits far from both (~0s skipped vs ~5s capped).
	if elapsed < 3*time.Second {
		t.Fatalf("handleActivity returned in %v after cancel, want >=3s (cancellation-path join must wait — boundedly, not skip — for the stuck renewal)", elapsed)
	}
	if n := store.landedCount(); n != 0 {
		t.Fatalf("stuck renewal landed %d times before the handler returned, want 0 (cancellation path must give up, not join, the blocked renewal)", n)
	}
	// The fix: no release may be issued while the renewal is still live.
	// The local lease (15s) is still fresh here (~5s elapsed of 15s), so
	// without the fix claimReleaseOwnership succeeds and ReleaseLease
	// lands before the stuck renewal.
	if n := store.releases.Load(); n != 0 {
		t.Fatalf("ReleaseLease issued %d times while an ordinary renewal was still live, want 0 (timed-out join must suppress the release; the task is left to natural expiry)", n)
	}
	w.mu.Lock()
	_, stillTracked := w.inFlight[task.ID]
	w.mu.Unlock()
	if stillTracked {
		t.Fatal("task still tracked after the give-up (must be untracked without release so no later path releases under the live renewal)")
	}

	select {
	case w.actSem <- struct{}{}:
		<-w.actSem
	default:
		t.Fatal("activity semaphore slot still held after the handler returned")
	}

	// Release the stuck renewal: it lands against an untracked entry and
	// a dropped guard, so it must refresh nothing — and still no release
	// may follow it.
	close(store.release)
	deadline := time.Now().Add(15 * time.Second)
	for store.landedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if store.landedCount() == 0 {
		t.Fatal("stuck renewal never landed after the gate release")
	}
	time.Sleep(200 * time.Millisecond)
	if n := store.releases.Load(); n != 0 {
		t.Fatalf("ReleaseLease issued %d times after the late renewal landed, want 0 (no release under a live renewal, before or after it lands)", n)
	}
	w.mu.Lock()
	_, reTracked := w.inFlight[task.ID]
	w.mu.Unlock()
	if reTracked {
		t.Fatal("task re-tracked by the late renewal (a stale renewal must refresh nothing)")
	}
}

// gateRetryBackend holds RetryActivity on a test-controlled gate once armed,
// so the test can keep a row-preserving result write open across renewal
// ticks while counting cover-relevant store calls.
type gateRetryBackend struct {
	backend.Backend
	mu           sync.Mutex
	extendCalls  int
	retryCalls   int
	retryEntered chan struct{}
	retryOnce    atomic.Bool
	retryRelease chan struct{}
}

func (b *gateRetryBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.extendCalls++
	b.mu.Unlock()
	return b.Backend.ExtendLease(ctx, task, d)
}

func (b *gateRetryBackend) RetryActivity(ctx context.Context, task backend.Task, delay time.Duration) error {
	if b.retryOnce.CompareAndSwap(false, true) {
		close(b.retryEntered)
	}
	<-b.retryRelease
	if ctx.Err() != nil {
		return ctx.Err()
	}
	b.mu.Lock()
	b.retryCalls++
	b.mu.Unlock()
	return b.Backend.RetryActivity(ctx, task, delay)
}

func (b *gateRetryBackend) counts() (extend, retry int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.extendCalls, b.retryCalls
}

// TestWorker_RowPreservingWriteHeldAgainstCover is the regression
// test for round-20 P1b (keep row-preserving writes ordered after cover
// renewals). A cover ExtendLease blocked in a context-ignoring backend when
// the result write completes ignores the post-write cover cancel and lands
// after it, overwriting the retry delay — and the bounded post-commit join
// gives up instead of ordering it.
//
// With the fix the commit sets the writing hold before the write, so cover
// ticks arriving mid-write skip issuing: no cover call overlaps the write.
// Layout: the RetryActivity write is held open across renewal ticks (lease
// 6s, tick 3s); with the fix the ticks issue nothing, so exactly one
// ExtendLease (the synchronous pre-commit renewal) ever runs. Without the
// fix a tick issues a second ExtendLease mid-write, which lands after the
// write once released and overwrites it.
func TestWorker_RowPreservingWriteHeldAgainstCover(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &gateRetryBackend{
		Backend:      mem,
		retryEntered: make(chan struct{}),
		retryRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          6 * time.Second,
		WorkerID:               "w1",
		ActivityConcurrency:    4,
		IncompatibleRetryDelay: -1,
	})
	// Retryable failure (MaxAttempts unset): the handler commits via
	// RetryActivity, the row-preserving write under test.
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "", errors.New("boom")
	}, WithName("flaky"))
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round20-hold-1", "flaky")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	herrCh := make(chan error, 1)
	go func() {
		herrCh <- w.handleActivity(ctx, task, tok)
	}()
	// Hold the write open long enough for at least one renewal tick
	// (tick at ~3s) to fire mid-write.
	select {
	case <-store.retryEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("RetryActivity never started")
	}
	time.Sleep(4500 * time.Millisecond)
	close(store.retryRelease)

	select {
	case herr := <-herrCh:
		if herr != nil {
			t.Fatalf("handleActivity = %v, want nil (held write must commit once cover is held)", herr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("handleActivity did not return after the write gate released")
	}
	if extend, retry := store.counts(); extend != 1 || retry != 1 {
		t.Fatalf("ExtendLease calls = %d, RetryActivity calls = %d, want (1, 1) (one sync pre-commit renewal, one held write, no cover issued mid-write)", extend, retry)
	}
}

// TestWorker_CoverJoinAbortsWriteOnTimeout is the regression test
// for the second half of round-20 P1b: when a cover renewal is already
// blocked as the row-preserving write approaches, the bounded pre-write
// cover join must give up AND the commit must abort instead of writing
// under the live renewal.
//
// Layout: direct guardedDetachedCommit drive (no activity). A cover renewal
// is planted and blocked in the backend first; the exclusive commit then
// has a live cover across its whole join budget, so it must return
// errLeaseLost without invoking the store op. Without the fix there is no
// cover join and the op runs under the live renewal.
func TestWorker_CoverJoinAbortsWriteOnTimeout(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &stallExtendCountingBackend{
		Backend: mem,
		stall:   func(call int) bool { return true },
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          30 * time.Second,
		WorkerID:               "w1",
		ActivityConcurrency:    4,
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round20-abort-1", "setup-flaky")
	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing, context.Background()) {
		t.Fatal("beginDetachedCommit refused a live tracked claim")
	}
	// Plant a cover renewal and wait until it is genuinely blocked in
	// the backend (registered in the barrier, unreleasable by cancel).
	coverDone := make(chan error, 1)
	go func() {
		coverDone <- w.renewOnceDetached(ctx, task.ID, tok)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.detMu.Lock()
		n := 0
		if e := w.coverInflight[task.ID]; e != nil {
			n = e.count
		}
		w.detMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			close(store.release)
			t.Fatal("planted cover renewal never registered in the barrier")
		}
		time.Sleep(2 * time.Millisecond)
	}
	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		close(store.release)
		t.Fatal("planted cover renewal never entered ExtendLease")
	}

	var opCalls atomic.Int32
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer commitCancel()
	start := time.Now()
	err := w.guardedDetachedCommit(task.ID, tok, commitCtx, commitCancel, true, func() error {
		opCalls.Add(1)
		return nil
	})
	elapsed := time.Since(start)
	if !errors.Is(err, errLeaseLost) {
		close(store.release)
		t.Fatalf("guardedDetachedCommit = %v, want errLeaseLost (a row-preserving write under a live cover renewal must abort, not write)", err)
	}
	if n := opCalls.Load(); n != 0 {
		close(store.release)
		t.Fatalf("store op invoked %d times under a live cover renewal, want 0 (timed-out cover join must abort the write)", n)
	}
	// The cover join must actually engage — not skip the wait — so the
	// floor sits far from both (~0s skipped vs ~5s capped).
	if elapsed < 3*time.Second {
		close(store.release)
		t.Fatalf("guardedDetachedCommit returned in %v, want >=3s (pre-write cover join must wait — boundedly, not skip — for the stuck cover)", elapsed)
	}

	// Release the stuck cover: it lands against a dropped guard, so it
	// must exit quietly and drain the barrier without refreshing
	// anything.
	close(store.release)
	select {
	case cerr := <-coverDone:
		if !errors.Is(cerr, errLeaseLost) {
			t.Fatalf("planted cover renewal = %v, want errLeaseLost (guard dropped by the abort)", cerr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("planted cover renewal never returned after the gate release")
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		w.detMu.Lock()
		_, barrierLeft := w.coverInflight[task.ID]
		_, guardLeft := w.detGuard[task.ID]
		w.detMu.Unlock()
		if !barrierLeft && !guardLeft {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cover barrier/guard not drained after the stuck cover landed (barrier must drain on every path)")
		}
		time.Sleep(2 * time.Millisecond)
	}
	w.mu.Lock()
	_, stillTracked := w.inFlight[task.ID]
	w.mu.Unlock()
	if stillTracked {
		t.Fatal("task still tracked after the aborted commit (commit transferred it out; nothing re-tracked it)")
	}
}
