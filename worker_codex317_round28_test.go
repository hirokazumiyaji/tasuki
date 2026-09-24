package tasuki

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// round28StallExtendBackend blocks the first ExtendLease while IGNORING the
// passed context, simulating a stalled store. The block is released by
// closing release (test cleanup); the commit path must already have
// returned via its commit-context bound.
type round28StallExtendBackend struct {
	backend.Backend
	entered  chan struct{}
	once     atomic.Bool
	release  chan struct{}
	extends  atomic.Int32
	retries  atomic.Int32
	releases atomic.Int32
}

func (b *round28StallExtendBackend) ExtendLease(_ context.Context, _ int64, _ time.Duration) error {
	b.extends.Add(1)
	if b.once.CompareAndSwap(false, true) {
		close(b.entered)
	}
	<-b.release
	return errors.New("round28 released")
}

func (b *round28StallExtendBackend) RetryActivity(ctx context.Context, id int64, delay time.Duration) error {
	b.retries.Add(1)
	return b.Backend.RetryActivity(ctx, id, delay)
}

func (b *round28StallExtendBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_Round28_ActivityPreCommitBoundedByCommitContext is the
// regression test for round-28 P1a (bound the activity pre-commit renewal
// by the commit context). Every activity-result path used to block in the
// synchronous pre-commit renewal BEFORE creating its commit context, with
// the detached renewal deriving only background cover ctx + lease timeout:
// a stalled ExtendLease held the activity slot past the advertised commit
// bound (ctx-aware backend) or indefinitely (ctx-ignoring backend,
// including across Shutdown/restart).
//
// Layout: 30s lease (renewal's own timeout far away), 300ms CommitTimeout,
// ExtendLease stalled ignoring ctx, activity fails retryably. The handler
// must return within the commit bound with errLeaseLost and issue no
// RetryActivity write. Without the fix the synchronous handoff blocks
// until the test releases the stall (flushDone timeout).
func TestWorker_Round28_ActivityPreCommitBoundedByCommitContext(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round28-precommit-1", "hooked")

	store := &round28StallExtendBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer close(store.release)
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 30 * time.Second,
		CommitTimeout: 300 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "", errors.New("boom-retryable")
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	done := make(chan error, 1)
	go func() { done <- w.handleActivity(ctx, task, tok) }()

	select {
	case <-store.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("pre-commit renewal did not start")
	}
	select {
	case herr := <-done:
		if !errors.Is(herr, errLeaseLost) {
			t.Fatalf("handleActivity = %v, want errLeaseLost (stalled pre-commit renewal must abort the commit within the commit bound)", herr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleActivity did not return within 5s while ExtendLease stalled ignoring ctx (pre-commit renewal must be bounded by min(commit bound, lease logic), not block past CommitTimeout)")
	}
	if n := store.retries.Load(); n != 0 {
		t.Fatalf("RetryActivity calls = %d, want 0 (unproven continuity must skip the store op)", n)
	}
	if n := store.extends.Load(); n == 0 {
		t.Fatal("ExtendLease calls = 0, want >= 1 (the pre-commit renewal must run; the abort must come from the bound, not a missing renewal)")
	}
}

// round28HookExtendBackend succeeds immediately while counting
// releases/commits for the Phase 1 race test.
type round28HookExtendBackend struct {
	backend.Backend
	releases atomic.Int32
	commits  atomic.Int32
}

func (b *round28HookExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	return b.Backend.ExtendLease(ctx, taskID, d)
}

func (b *round28HookExtendBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

func (b *round28HookExtendBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.commits.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

// TestWorker_Round28_Phase1SuccessRacingTimeoutCompensates is the
// regression test for round-28 P2a (synchronize timeout fencing with
// renewal completion). An initial ExtendLease that succeeds concurrently
// with the flush ctx expiring used to trip the guard (covered==false at
// the timeout check) and then mark covered without compensation: commit
// gates reject the missing guard, but the successful renewal already hid
// the unowned task for a full lease.
//
// Layout: 30s lease, 300ms flush bound, ExtendLease succeeding
// immediately. round28CoverStoreHook pauses the success goroutine between
// its nil return (guard present) and its covered store, forcing the
// timeout fence to win deterministically; the resuming goroutine must
// compensate with a fenced release and leave the entry uncovered (no
// commit, no Phase 2 loop). Without the fix it marks covered without
// compensation (task stays hidden, release missing).
func TestWorker_Round28_Phase1SuccessRacingTimeoutCompensates(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	const lease = 30 * time.Second
	store := &round28HookExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: lease,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	claimStart := time.Now()
	task, tok, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round28-fence-1", lease, claimStart)
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, tok)

	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	var hookOnce atomic.Bool
	oldHook := round28CoverStoreHook
	round28CoverStoreHook = func() {
		if hookOnce.CompareAndSwap(false, true) {
			close(hookEntered)
		}
		<-hookRelease
	}
	defer func() { round28CoverStoreHook = oldHook }()

	commitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(commitCtx, []pendingWorkflowCommit{pending})
	}()

	// The renewal succeeds immediately and parks in the hook with the
	// guard still present (nil return, store not yet run).
	select {
	case <-hookEntered:
	case <-time.After(15 * time.Second):
		close(hookRelease)
		t.Fatal("Phase 1 renewal did not reach the store hook")
	}
	// Let the flush bound expire and the timeout fence trip (covered
	// still false, guard deleted) before resuming the success path.
	select {
	case <-commitCtx.Done():
	case <-time.After(15 * time.Second):
		close(hookRelease)
		t.Fatal("flush commit context did not expire")
	}
	time.Sleep(100 * time.Millisecond)
	close(hookRelease)

	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after the hooked success resumed (must be bounded)")
	}
	if n := store.commits.Load(); n != 0 {
		t.Fatalf("CommitAdvancement calls = %d, want 0 (tripped entry must admit no commit)", n)
	}
	// The compensation lands after the hook release (the success
	// goroutine was parked past the flush return): poll for it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := store.releases.Load(); n != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ReleaseLease calls = 0, want >= 1 (successful-but-tripped renewal must be compensated with a fenced release; without it the task stays hidden a full lease with no commit admitted)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round28-fence-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore {
		t.Fatalf("head nextSeq = %d, want %d (tripped entry must not apply)", stAfter.NextSeq, nextBefore)
	}
}

// round28FailCommitStallCoverBackend fails the commit while a periodic
// cover renewal stays blocked ignoring cancellation, exercising the
// requeue join give-up path.
type round28FailCommitStallCoverBackend struct {
	backend.Backend
	extendCalls atomic.Int32
	nacks       atomic.Int32
	releases    atomic.Int32
	coverGate   chan struct{}
	coverDone   chan struct{}
	once        atomic.Bool
}

func (b *round28FailCommitStallCoverBackend) ExtendLease(_ context.Context, taskID int64, d time.Duration) error {
	n := b.extendCalls.Add(1)
	if n == 1 {
		// Initial renewal: succeed so the commit is admitted and fails.
		return b.Backend.ExtendLease(context.Background(), taskID, d)
	}
	// Periodic cover: block ignoring cancellation until the test ends.
	if b.once.CompareAndSwap(false, true) {
		close(b.coverGate)
	}
	<-b.coverDone
	return errors.New("round28 cover released")
}

func (b *round28FailCommitStallCoverBackend) CommitAdvancement(_ context.Context, _ backend.Advancement) error {
	// Hold the commit until the periodic cover is blocked in the
	// backend, so the failure-path join cannot drain: the requeue must
	// then be suppressed, not issued under the live renewal.
	select {
	case <-b.coverGate:
	case <-time.After(15 * time.Second):
		return errors.New("round28 setup: periodic cover never started")
	}
	return errors.New("round28 injected commit failure")
}

func (b *round28FailCommitStallCoverBackend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	b.nacks.Add(1)
	return b.Backend.NackTask(ctx, t, delay)
}

func (b *round28FailCommitStallCoverBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_Round28_FailedCommitSkipsRequeueWhenCoverLive is the
// regression test for round-28 P2b (skip requeue when the cover-renewal
// join times out). A periodic cover renewal ignoring cancel past the
// commit bound used to be followed by an immediate release/nack: the late
// ExtendLease then landed after it, replacing visible_at and hiding the
// retry for a full lease.
//
// Layout: initial cover succeeds, CommitAdvancement fails transiently,
// periodic cover blocks ignoring ctx past the join bound. The failure
// path must suppress the requeue (no NackTask/ReleaseLease) and leave the
// task to natural expiry. Without the fix the requeue issues immediately
// and the late renewal overwrites it.
func TestWorker_Round28_FailedCommitSkipsRequeueWhenCoverLive(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	const lease = 400 * time.Millisecond
	store := &round28FailCommitStallCoverBackend{
		Backend:   mem,
		coverGate: make(chan struct{}),
		coverDone: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          lease,
		WorkerID:               "w1",
		IncompatibleRetryDelay: time.Minute,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	claimStart := time.Now()
	task, tok, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round28-requeue-1", lease, claimStart)
	pending := round23Pending(task, st, tok)

	// Bounded flush ctx so the deferred Phase 2 stop join cannot hold
	// the flush past the bound while the periodic cover stays stalled
	// (with a Background ctx that join is unbounded by design).
	flushCtx, flushCancel := context.WithTimeout(ctx, 12*time.Second)
	defer flushCancel()
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(flushCtx, []pendingWorkflowCommit{pending})
	}()

	// Wait until the periodic cover is blocked in the backend, so the
	// failure-path join cannot drain.
	select {
	case <-store.coverGate:
	case <-time.After(15 * time.Second):
		t.Fatal("periodic cover renewal did not start")
	}
	// Give the failed commit a moment to reach its join; the join itself
	// is bounded by commitJoinCap (5s) with a live flush ctx.
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		close(store.coverDone)
		t.Fatal("flush did not return while periodic cover stalled (join must be bounded)")
	}
	close(store.coverDone)
	if n := store.nacks.Load(); n != 0 {
		t.Fatalf("NackTask calls = %d, want 0 (join give-up must suppress the requeue; the live cover would otherwise overwrite visible_at and hide the retry)", n)
	}
	if n := store.releases.Load(); n != 0 {
		t.Fatalf("ReleaseLease calls = %d, want 0 (join give-up must suppress the requeue)", n)
	}
}
