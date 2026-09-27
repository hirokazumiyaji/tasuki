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
	"github.com/hirokazumiyaji/tasuki/journal"
)

// blockExtendBackend blocks selected ExtendLease calls on a
// test-controlled gate while counting calls and completions, so tests can
// hold a renewal past the lease continuity deadline (round-10 P1c) or fail
// cover renewals outright (round-10 P1b). Calls not matching blockFirst
// delegate to failRest (nil = delegate success).
type blockExtendBackend struct {
	backend.Backend
	mu         sync.Mutex
	calls      int
	exits      int
	entered    chan struct{}
	enterOnce  atomic.Bool
	release    chan struct{}
	blockFirst bool
	failRest   error
}

func newBlockExtendBackend(mem *memory.Backend, blockFirst bool, failRest error) *blockExtendBackend {
	return &blockExtendBackend{
		Backend:    mem,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
		blockFirst: blockFirst,
		failRest:   failRest,
	}
}

func (b *blockExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	first := b.calls == 1
	b.mu.Unlock()
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	if first && b.blockFirst {
		// Block WITHOUT honoring ctx: the point is a renewal the
		// backend holds past the lease (backends may ignore
		// cancellation). Honoring the renewal's own lease-bounded
		// timeout here would fail the call before the release and
		// test the timeout path instead of the late-success path.
		// The wall-clock fallback keeps a test bug from hanging
		// the suite.
		select {
		case <-b.release:
		case <-time.After(15 * time.Second):
			return errors.New("test stalled: extend release never closed")
		}
		b.mu.Lock()
		b.exits++
		b.mu.Unlock()
		return b.Backend.ExtendLease(ctx, task, d)
	}
	if b.failRest != nil {
		return b.failRest
	}
	b.mu.Lock()
	b.exits++
	b.mu.Unlock()
	return b.Backend.ExtendLease(ctx, task, d)
}

func (b *blockExtendBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// TestWorker_Round10_RenewalCompletingPastDeadlineAbortsCommit is the
// regression test for round-10 P1c (continuity through completion): the
// pre-commit detached renewal STARTS before the continuity deadline but
// only COMPLETES after it, blocked in the backend past a potential
// expiry-and-reclaim gap. The ID-only ExtendLease reports success while
// extending whatever lease is there now — possibly a peer's — so the
// call-start guard accepts and the stale commit would proceed. The fix
// trips success-followed-by-loss the same way as the start gap.
//
// Without the fix ensureCommitRenewal returns nil (start < deadline,
// success refreshes); with the fix it returns errLeaseLost after
// attempting the renewal (calls>=1 proves this is the success path, not
// the round-8 failure path).
func TestWorker_Round10_RenewalCompletingPastDeadlineAbortsCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round10-late-1", "hooked")
	store := newBlockExtendBackend(mem, true, nil)
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})

	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)
	var committing atomic.Bool
	if !w.beginDetachedCommit(task.ID, tok, &committing, context.Background()) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	// Release the renewal 400ms in — well past the 200ms continuity
	// deadline — so it starts fresh but completes stale.
	time.AfterFunc(400*time.Millisecond, func() { close(store.release) })

	renewDone := make(chan struct{})
	defer close(renewDone)
	detachedEntered := make(chan struct{})
	defer close(detachedEntered)
	start := time.Now()
	_, err := w.ensureCommitRenewal(ctx, task.ID, tok, renewDone, detachedEntered)
	t.Logf("ensureCommitRenewal blocked %v", time.Since(start))
	if err == nil {
		t.Fatal("ensureCommitRenewal = nil, want errLeaseLost (a renewal completing past the deadline is not ownership)")
	}
	if !errors.Is(err, errLeaseLost) {
		t.Fatalf("ensureCommitRenewal = %v, want errLeaseLost", err)
	}
	if n := store.callCount(); n == 0 {
		t.Fatal("ExtendLease calls=0, want >=1 (the renewal must be attempted; its late success is what gets rejected)")
	}
}

// TestWorker_Round10_ObservedLossAbortsInflightCommit is the regression
// test for round-10 P1b (pre-commit loss plumbing): the synchronous
// pre-commit renewal is still blocked in the backend when a periodic
// cover renewal observes lease loss (task row gone). The cover loop
// exits, but the handler's Complete/Retry/nack runs on an independent
// commit context — without the fix it proceeds onto a task the worker
// no longer owns. With the fix the late pre-commit success finds the
// tripped (dropped) guard and aborts instead of touching the store.
//
// Without the fix handleActivity returns nil and CompleteActivity is
// attempted (completes>=1); with the fix it returns errLeaseLost and
// the store op is never issued.
func TestWorker_Round10_ObservedLossAbortsInflightCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          120 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round10-lost-1", "hooked")
	// First renewal (the handler's synchronous pre-commit) blocks; every
	// later cover renewal reports the row gone.
	store := newBlockExtendBackend(mem, true, backend.ErrNotFound)
	counting := &countExtendBackend{Backend: store}
	w := NewWorker(counting, WorkerOptions{
		LeaseDuration:          120 * time.Millisecond, // 60ms cover tick
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "ok", nil
	}, WithName("hooked"))

	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(ctx, task, tok) }()

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-commit renewal never started")
	}
	// The 60ms cover tick fires while the pre-commit renewal is still
	// blocked, observes the loss, trips the guard, and exits.
	deadline := time.Now().Add(5 * time.Second)
	for store.callCount() < 2 {
		if time.Now().After(deadline) {
			close(store.release)
			t.Fatal("cover renewal never ran while the pre-commit renewal was blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond) // let the trip land
	close(store.release)               // pre-commit renewal completes (stale)

	select {
	case herr := <-herrCh:
		if herr == nil {
			t.Fatal("handleActivity = nil, want errLeaseLost (cover observed loss while the pre-commit renewal was in flight)")
		}
		if !errors.Is(herr, errLeaseLost) {
			t.Fatalf("handleActivity = %v, want errLeaseLost", herr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("handler did not return after the pre-commit renewal completed stale")
	}
	if n := counting.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls=%d, want 0 (commit must be skipped once loss was observed)", n)
	}
}

// TestWorker_Round10_LossDuringBlockedCommitCancelsStoreOp covers the
// second half of round-10 P1b: the pre-commit renewal succeeded and the
// store op itself is blocked in the backend when a cover renewal
// observes lease loss. Gating already passed, so only aborting the
// in-flight call helps: the trip cancels the stashed commit context and
// a context-aware backend op fails instead of applying a stale write
// onto the peer's reclaimed task.
//
// Without the fix the blocked CompleteActivity applies once released
// (task row gone, probe finds nothing); with the fix the trip cancels
// it mid-block and the task stays claimable after expiry.
func TestWorker_Round10_LossDuringBlockedCommitCancelsStoreOp(t *testing.T) {
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
	// Cover renewals pass through until armed, then report the row gone.
	cover := &armableExtendBackend{Backend: gated}
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
	task := setupClaimableActivityTask(t, ctx, cover, mem, setupW, "round10-cancel-1", "hooked")
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "ok", nil
	}, WithName("hooked"))

	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	gated.armCommit.Store(true)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(ctx, task, tok) }()
	select {
	case <-gated.commitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached commit did not start")
	}
	// The commit is blocked; arm cover failure so the next 60ms tick
	// observes loss, trips the guard, and cancels the commit context.
	// The gated CompleteActivity selects on ctx.Done, so the cancel
	// unblocks it without applying the write.
	cover.armFail.Store(true)
	select {
	case herr := <-herrCh:
		if herr == nil {
			t.Fatal("handleActivity = nil, want a cancelation error (loss during the blocked commit must abort the store op)")
		}
		t.Logf("handleActivity = %v", herr)
	case <-time.After(8 * time.Second):
		t.Fatal("handler did not return after the mid-commit loss trip (commit was not canceled)")
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

// cancelAwareCommitBackend pins an armed CompleteActivity on a
// test-controlled gate that ALSO observes context cancellation (unlike
// gateCommitBackend, which blocks unconditionally): a mid-commit trip
// cancel unblocks it with ctx.Err() and the write never applies, proving
// the in-flight commit was aborted rather than merely delayed.
type cancelAwareCommitBackend struct {
	backend.Backend
	armCommit     atomic.Bool
	commitOnce    atomic.Bool
	commitEntered chan struct{}
	commitRelease chan struct{}
}

func (b *cancelAwareCommitBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	if b.armCommit.Load() {
		if b.commitOnce.CompareAndSwap(false, true) {
			close(b.commitEntered)
		}
		select {
		case <-b.commitRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

// armableExtendBackend passes ExtendLease through until armed, then fails
// every call with ErrNotFound (task row gone) to simulate a cover
// renewal observing lease loss mid-commit.
type armableExtendBackend struct {
	backend.Backend
	armFail atomic.Bool
}

func (b *armableExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	if b.armFail.Load() {
		return backend.ErrNotFound
	}
	return b.Backend.ExtendLease(ctx, task, d)
}

// TestWorker_Round10_CommitStopJoinsInheritedLoop is the regression test
// for round-10 P1a (join the inherited renewal loop after result
// commits): the deferred stop must terminate AND join the inherited
// loop before the handler drops its guard — signal stop, wait for loop
// exit, then proceed — so no ExtendLease issued for the commit can land
// after the result op and overwrite what it wrote. A no-op stop
// returns while the loop is still alive.
//
// This drives the wrapper directly: renewDone stays open (inherited
// loop alive) until the test closes it, so a no-op stop would return
// immediately with done still open.
func TestWorker_Round10_CommitStopJoinsInheritedLoop(t *testing.T) {
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	defer closeDone()
	renewDone := make(chan struct{})

	var stopped atomic.Bool
	stop := joinCommitStop(func() { stopped.Store(true) }, closeDone, renewDone)

	// Simulate the inherited loop exiting 100ms after the stop: the
	// wrapper must wait for it instead of returning on the open channel.
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(renewDone)
	}()
	start := time.Now()
	stop()
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("commit stop returned in %v, want >=80ms (must join the inherited loop's exit, not assume it)", elapsed)
	}
	if !stopped.Load() {
		t.Fatal("scoped stop was not invoked")
	}
	select {
	case <-done:
	default:
		t.Fatal("handler done was not closed by the commit stop (loop was not signaled)")
	}
	// Idempotent: a second call must return immediately without panic.
	stop()
}
