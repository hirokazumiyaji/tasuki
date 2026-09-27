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

// transientArmExtendBackend passes ExtendLease through until armed, then
// fails every call with a transient error (ownership unknown) to simulate
// a detached cover renewal hitting a store timeout mid-commit. Unlike
// armableExtendBackend (ErrNotFound, definitively moved on), a transient
// failure used to keep the periodic cover alive.
type transientArmExtendBackend struct {
	backend.Backend
	armFail atomic.Bool
}

func (b *transientArmExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	if b.armFail.Load() {
		return errors.New("transient store timeout")
	}
	return b.Backend.ExtendLease(ctx, task, d)
}

// TestWorker_Round11_TransientCoverFailureAbortsBlockedCommit is the
// regression test for round-11 P1a (abort commits after periodic renewal
// errors): a periodic detached renewal failing with a TRANSIENT error —
// not errLeaseLost — near the continuity deadline must still cancel the
// result commit. The lease can expire and be reclaimed before the next
// half-lease tick, and the ID-only Complete that follows would modify
// the peer's task. With the fix any detached-renewal failure trips the
// guard and cancels the in-flight commit via the stashed commit cancel.
//
// Without the fix the periodic loop ignores non-lease-lost errors and
// keeps the cover alive: no trip fires, the blocked Complete is never
// canceled, and the handler does not return within the timeout.
func TestWorker_Round11_TransientCoverFailureAbortsBlockedCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	gated := &cancelAwareCommitBackend{
		Backend:       mem,
		commitEntered: make(chan struct{}),
		commitRelease: make(chan struct{}),
	}
	defer close(gated.commitRelease)
	// Cover renewals pass through until armed, then fail transiently.
	cover := &transientArmExtendBackend{Backend: gated}
	w := NewWorker(cover, WorkerOptions{
		LeaseDuration:          120 * time.Millisecond, // 60ms cover tick
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          120 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, cover, mem, setupW, "round11-transient-1", "hooked")
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "ok", nil
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	gated.armCommit.Store(true)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(ctx, task, tok) }()
	select {
	case <-gated.commitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached commit did not start")
	}
	// The commit is blocked; arm transient cover failure so the next
	// 60ms tick trips the guard and cancels the commit context. The
	// gated CompleteActivity selects on ctx.Done, so the cancel unblocks
	// it without applying the write.
	cover.armFail.Store(true)
	select {
	case herr := <-herrCh:
		if herr == nil {
			t.Fatal("handleActivity = nil, want a cancelation error (transient cover failure must abort the blocked commit)")
		}
		t.Logf("handleActivity = %v", herr)
	case <-time.After(6 * time.Second):
		t.Fatal("handler did not return after the transient cover failure (commit was not canceled)")
	}
	// The stale write must not have landed: past the original lease the
	// task row is still there and claimable. A completed task would be
	// deleted and this probe would find nothing.
	mem.SetNow(t0.Add(10 * time.Second))
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 1 {
		t.Fatalf("probe claimed %d tasks, want 1 (canceled commit must not delete the task)", len(probe))
	}
}

// stallExtendBackend blocks every ExtendLease on a test-controlled gate
// while counting wrapper calls and landed backend writes, so a test can
// hold a renewal in flight across a result commit and observe whether
// its write lands. The gate ALSO observes context cancellation (unlike
// blockExtendBackend): a renewal aborted by the commit's cover cancel
// returns ctx.Err() without delegating, proving its write was dropped
// rather than merely delayed.
type stallExtendBackend struct {
	backend.Backend
	mu        sync.Mutex
	calls     int
	landed    int
	entered   chan struct{}
	enterOnce atomic.Bool
	release   chan struct{}
}

func (b *stallExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	// A well-behaved context-aware backend checks cancellation first:
	// a write aborted before it starts never lands.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Second):
		return errors.New("test stalled: extend release never closed")
	}
	b.mu.Lock()
	b.landed++
	b.mu.Unlock()
	return b.Backend.ExtendLease(ctx, task, d)
}

func (b *stallExtendBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *stallExtendBackend) landedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.landed
}

// TestWorker_Round11_ExclusiveCommitAbortsOnStuckCover is the successor
// of the round-11 P1b test (no renewal write after the result write),
// updated for round-20 P1b. The old contract let a row-preserving result
// commit (RetryActivity/nack, which rewrite visible_at in place) run
// while a cover renewal was still blocked, relying on the post-write
// cover cancel to drop it. That cancel is best-effort: a
// context-ignoring backend lands the renewal after the write anyway,
// overwriting what it wrote, and the bounded post-commit join gives up
// instead of ordering it.
//
// The new contract joins admitted cover renewals BEFORE the write and
// aborts when the bounded join gives up: a periodic renewal blocked in
// the backend across the commit's whole join budget means the commit
// returns errLeaseLost WITHOUT running its store op. Landing after an
// abort only extends the lease, while landing after a write would
// overwrite it. A renewal blocked across the budget then released lands
// against a dropped guard and exits quietly with lease loss.
//
// Layout mirrors the old test: a cover renewal is planted and blocked,
// then the exclusive commit must abort (not run) after the join budget.
// The context-aware stall backend still proves the old backstop: the
// abort's cover cancel drops the blocked write (zero landed writes).
func TestWorker_Round11_ExclusiveCommitAbortsOnStuckCover(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &stallExtendBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round11-serial-1", "hooked")

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing, context.Background()) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}

	// A cover renewal enters ExtendLease and blocks there.
	renewCh := make(chan error, 1)
	go func() { renewCh <- w.renewOnceDetached(ctx, task.ID, tok) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cover renewal never entered ExtendLease")
	}

	// A row-preserving result commit approaches while the renewal is
	// still blocked. It must NOT run under the live renewal: the
	// bounded pre-write cover join gives up and the commit aborts with
	// errLeaseLost, leaving the task untracked for natural expiry.
	commitCtx, commitCancel := context.WithCancel(context.Background())
	defer commitCancel()
	var opDone atomic.Bool
	commitCh := make(chan error, 1)
	start := time.Now()
	go func() {
		commitCh <- w.guardedDetachedCommit(task.ID, tok, commitCtx, commitCancel, true, func() error {
			opDone.Store(true)
			return mem.RetryActivity(commitCtx, task.ID, 5*time.Second)
		})
	}()
	select {
	case cerr := <-commitCh:
		if !errors.Is(cerr, errLeaseLost) {
			t.Fatalf("exclusive commit = %v, want errLeaseLost (a row-preserving write under a live cover renewal must abort, not write)", cerr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("exclusive commit did not abort while the renewal was blocked (pre-write cover join must be bounded by commitJoinCap)")
	}
	// The cover join must actually engage — not skip the wait — so the
	// floor sits far from both (~0s skipped vs ~5s capped).
	if elapsed := time.Since(start); elapsed < 3*time.Second {
		t.Fatalf("exclusive commit returned in %v, want >=3s (pre-write cover join must wait — boundedly, not skip — for the stuck cover)", elapsed)
	}
	if opDone.Load() {
		t.Fatal("exclusive commit op ran under a live cover renewal (timed-out cover join must abort the write)")
	}
	// The abort's cover cancel drops the still-blocked renewal: it
	// returns lease loss without delegating to the backend. The wait
	// bound (3s) sits well under the renewal's own 10s lease timeout,
	// so only the abort's cancel — not the timeout backstop — can
	// pass this.
	select {
	case rerr := <-renewCh:
		if !errors.Is(rerr, errLeaseLost) {
			t.Fatalf("aborted cover renewal = %v, want errLeaseLost", rerr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked cover renewal was not aborted by the commit's cover cancel")
	}
	if n := store.landedCount(); n != 0 {
		t.Fatalf("landed ExtendLease writes=%d, want 0 (in-flight renewal must be dropped, not landed after the abort)", n)
	}

	// Releasing the gate now must change nothing: the write was
	// already dropped and no result write ever ran, so no retry delay
	// hides the task. At t0+3s the task is visible for natural reclaim
	// (the setup claim's short lease expired); a renewal landing after
	// a result write would instead have overwritten visible_at.
	close(store.release)
	time.Sleep(100 * time.Millisecond) // let a non-dropped write land, if any
	mem.SetNow(t0.Add(3 * time.Second))
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1 (aborted commit wrote nothing; the task is left to natural expiry reclaim)", len(probe))
	}
	if n := store.landedCount(); n != 0 {
		t.Fatalf("landed ExtendLease writes=%d, want 0 (released gate must not land a dropped write)", n)
	}

	// No renewal may issue after the abort: the guard is dropped, so a
	// late renewal reports loss without touching the backend.
	if rerr := w.renewOnceDetached(ctx, task.ID, tok); !errors.Is(rerr, errLeaseLost) {
		t.Fatalf("post-commit renewal = %v, want errLeaseLost", rerr)
	}
	if n := store.callCount(); n != 1 {
		t.Fatalf("ExtendLease calls=%d, want 1 (no renewal may issue after the abort)", n)
	}
}
