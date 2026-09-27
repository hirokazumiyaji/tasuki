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

// countIDReleaseBackend counts ReleaseLease calls on the fenced backend
// API while delegating to memory.
type countIDReleaseBackend struct {
	backend.Backend
	releases atomic.Int32
}

func (b *countIDReleaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_StaleGenerationClaimFenced is a regression test for the
// restart-generation finding: a Start-parent cancel plus a worker restart
// with spare concurrency lets a cancellation-ignoring invocation outlive
// its lease while the new generation reclaims and tracks the same task
// ID. The stale invocation's token must not match the new entry, so its
// untrack and ownership claims are no-ops and the new generation keeps
// its entry.
//
// Without the token fence the stale untrack deletes the new entry and the
// stale release claim succeeds, handing the new generation's lease to a
// peer while it still runs (concurrent execution).
func TestWorker_StaleGenerationClaimFenced(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})

	stale := w.track(42)
	// Simulate the restart: a new Start generation re-tracks the same
	// task ID (StartWithError bumps epoch; the reclaim stamps a new
	// token over the stale entry).
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	fresh := w.track(42)

	// The stale return must not disturb the new generation's entry.
	w.untrack(42, stale)
	if !w.owns(42, fresh) {
		t.Fatal("stale untrack deleted the new generation's entry (new claim left untracked and releasable by Shutdown)")
	}
	if w.claimReleaseOwnership(42, stale) {
		t.Fatal("stale invocation claimed release ownership of the new generation's lease (would clear a running peer's lease)")
	}
	if w.claimCommitOwnership(42, stale) {
		t.Fatal("stale invocation claimed commit ownership of the new generation's entry (would untrack a running claim)")
	}
	if !w.owns(42, fresh) {
		t.Fatal("new generation lost its entry to the stale invocation")
	}
	// The new generation still owns its lease end to end.
	if !w.claimReleaseOwnership(42, fresh) {
		t.Fatal("new generation lost release ownership of its own lease")
	}
	w.mu.Lock()
	_, ok := w.inFlight[42]
	w.mu.Unlock()
	if ok {
		t.Fatal("new generation's release did not remove its entry")
	}
}

// TestWorker_StaleGenerationReleaseSkipped is a regression test for the
// restart-generation finding on the shutdown-release path: the stale
// invocation returns after the new generation re-tracked its task ID and
// takes the canceled-context release path. The release must not land.
func TestWorker_StaleGenerationReleaseSkipped(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &countIDReleaseBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
		// The setup helper claims through this worker's PollOnce: an
		// unregistered-activity nack must stay visible so the helper can
		// grab the task.
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "stale-release-1", "ghost")

	stale := w.track(task.ID)
	// Restart + reclaim by the new generation.
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	fresh := w.track(task.ID)

	oldCtx, oldCancel := context.WithCancel(context.Background())
	oldCancel() // the stale generation's execution context is dead
	if err := w.handleActivity(oldCtx, task, stale); err == nil {
		t.Fatal("handleActivity = nil, want the stale context error")
	}
	if got := store.releases.Load(); got != 0 {
		t.Fatalf("ReleaseLease calls=%d, want 0 (stale invocation must not release the new generation's lease)", got)
	}
	if !w.owns(task.ID, fresh) {
		t.Fatal("new generation's entry is gone after the stale return (Shutdown would now release a running claim)")
	}
	// Cleanup: the new generation's shutdown release still works.
	if !w.claimReleaseOwnership(task.ID, fresh) {
		t.Fatal("new generation lost release ownership of its own lease")
	}
}

// TestWorker_StaleGenerationCommitFenced is a regression test for the
// restart-generation finding on the result-commit path: the stale
// invocation finishes (success) after the new generation re-tracked its
// task ID. Its commit transfer must fail without touching the new entry,
// so no task-ID-only CompleteActivity clobbers the new generation's task.
func TestWorker_StaleGenerationCommitFenced(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &countIDReleaseBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  time.Hour, // no background ticks; the handler owns renewal
		LeaseDuration: 10 * time.Second,
		WorkerID:      "w1",
		// See above: the setup helper needs the unregistered nack to stay
		// visible. ("hooked" is registered only after setup returns.)
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "stale-commit-1", "hooked")
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "ok", nil
	}, WithName("hooked"))

	stale := w.track(task.ID)
	// Restart + reclaim by the new generation.
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	fresh := w.track(task.ID)

	// The stale invocation runs under a live ctx (a cancellation-ignoring
	// activity that already returned before the cancel landed).
	if err := w.handleActivity(context.Background(), task, stale); err != nil {
		t.Fatalf("handleActivity = %v, want nil (stale result is dropped, not an error)", err)
	}
	if !w.owns(task.ID, fresh) {
		t.Fatal("new generation's entry is gone after the stale commit attempt (its task was clobbered or untracked)")
	}
	if got := store.releases.Load(); got != 0 {
		t.Fatalf("ReleaseLease calls=%d, want 0 (no release follows a dropped stale commit)", got)
	}
	// The new generation's task is untouched: the stale CompleteActivity
	// never ran, so the task is still leased, not completed.
	time.Sleep(100 * time.Millisecond) // let the handler's renewal loop exit on done
	w.untrack(task.ID, fresh)
}

// TestWorker_CommitOwnershipLossExitsDetachedMode is a regression test for
// the renewal-after-ownership-loss finding: grace expiry lands between the
// handler's live-ctx check and the commit ownership transfer, so
// releaseInFlight removes and releases first and the commit transfer
// fails. The handler must clear the committing flag AND join the renewal
// loop before returning — otherwise the loop enters detached mode and its
// immediate ExtendLease lands post-release (re-hiding the task or
// modifying a peer's fresh lease).
//
// The renewal loop is driven directly: it sits parked in a gated detached
// ExtendLease (exactly the state exitDetachedCommit must join), and the
// test asserts the flag clears while the renewal is still in flight and
// the join completes only after the renewal finishes.
func TestWorker_CommitOwnershipLossExitsDetachedMode(t *testing.T) {
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &joinOrderBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendGate:    make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 10 * time.Second, // 5s tick: no periodic renewal during the test
		WorkerID:      "w1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	var committing atomic.Bool
	// A detached commit is always entered via beginDetachedCommit, which
	// also seeds the renewal continuity guard: mirror the production
	// setup so the loop's cover renewal is owned (round-9 P1b).
	tok := w.track(42)
	defer w.untrack(42, tok)
	defer w.dropDetachedGuard(42, tok)
	if !w.beginDetachedCommit(42, tok, &committing, context.Background()) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	done := make(chan struct{})
	renewDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(renewDone)
		w.extendLeaseLoop(ctx, 42, tok, done, &committing)
	}()
	time.Sleep(50 * time.Millisecond) // renewal loop parked in select

	// Grace expiry: the execution context dies while the commit is in
	// flight, so the loop is about to enter (or has entered) detached
	// renewal.
	store.armBlock.Store(true)
	cancel()
	select {
	case <-store.extendEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal loop did not enter detached mode")
	}

	// Ownership lost (releaseInFlight removed and released first): the
	// handler must leave detached mode and join the loop.
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		w.exitDetachedCommit(ctx, renewDone, &committing)
	}()

	// The flag clears while the renewal is still in flight — a loop that
	// wakes after this point sees detached mode is over and stops.
	deadline := time.Now().Add(5 * time.Second)
	for committing.Load() {
		if time.Now().After(deadline) {
			close(store.extendGate)
			close(done)
			wg.Wait()
			t.Fatal("committing flag still set while the renewal is in flight (loop would keep renewing post-release)")
		}
		time.Sleep(time.Millisecond)
	}
	// The join must not complete before the in-flight renewal does: the
	// release ordered after this join can never be followed by a renewal.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-exited:
		close(store.extendGate)
		close(done)
		wg.Wait()
		t.Fatal("exitDetachedCommit returned while a renewal was still in flight (a later release could be re-hidden)")
	default:
	}
	close(store.extendGate)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		close(done)
		wg.Wait()
		t.Fatal("exitDetachedCommit did not return after the renewal finished")
	}
	close(done)
	wg.Wait()
	if a, b := store.indexOf("extend-exit"), store.indexOf("release-exit"); b >= 0 && !(a >= 0 && a < b) {
		t.Fatalf("lease op order = %v, want no renewal after the release", store.snapshot())
	}
}

// TestWorker_StartBumpsEpoch guards the generation dial itself: every
// StartWithError moves to a fresh epoch so tokens stamped before a
// restart never match entries tracked after it.
func TestWorker_StartBumpsEpoch(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		PollInterval:  time.Hour,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})

	stale := w.track(7)
	w.Start(ctx)
	w.untrack(7, stale) // the old invocation returned; keep Shutdown quiet
	w.mu.Lock()
	first := w.epoch
	w.mu.Unlock()
	if first == 0 {
		t.Fatal("epoch = 0 after Start, want a fresh generation")
	}
	shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shCancel()
	if err := w.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	w.Start(ctx)
	defer func() {
		shCtx2, shCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer shCancel2()
		_ = w.Shutdown(shCtx2)
	}()
	w.mu.Lock()
	second := w.epoch
	w.mu.Unlock()
	if second != first+1 {
		t.Fatalf("epoch = %d after restart, want %d (one fresh generation per Start)", second, first+1)
	}
	// A pre-restart token never matches a post-restart entry for the same
	// task ID.
	fresh := w.track(7)
	if w.owns(7, stale) {
		t.Fatal("pre-restart token matches the post-restart entry (stale invocation could release the new lease)")
	}
	if !w.owns(7, fresh) {
		t.Fatal("post-restart claim is not tracked")
	}
	w.untrack(7, fresh)
}
