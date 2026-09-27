package tasuki

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// extendRecorder records ExtendLease calls with the calling context's
// liveness. It returns nil without touching the store so no task needs to
// exist.
type extendRecorder struct {
	backend.Backend
	mu    sync.Mutex
	calls []extendCall
}

type extendCall struct {
	taskID int64
	live   bool
}

func (b *extendRecorder) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls = append(b.calls, extendCall{taskID: task.ID, live: ctx.Err() == nil})
	b.mu.Unlock()
	return nil
}

func (b *extendRecorder) extendCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

// TestExtendLeaseLoopRenewsImmediatelyOnDetachedEntry is a regression test
// for the detached-renewal gap: when execution cancel coincides with a
// renewal tick, the ticker branch Extends with the canceled ctx and fails,
// and the mode transition must not wait for the next half-lease tick (==
// the original lease expiry for the first renewal) before using a detached
// context — a peer would reclaim the still-committing task first.
func TestExtendLeaseLoopRenewsImmediatelyOnDetachedEntry(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &extendRecorder{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: 10 * time.Second, // 5s tick: no periodic renewal during the test
		WorkerID:      "w1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	var committing atomic.Bool
	// A detached commit is always entered via beginDetachedCommit, which
	// also seeds the renewal continuity guard: mirror the production
	// setup so the loop's cover renewal is owned (round-9 P1b).
	tok := w.trackTaskAt(backend.Task{ID: 42, Kind: "activity", WorkerID: "w1", Attempt: 1}, time.Now())
	defer w.untrack(42, tok)
	defer w.dropDetachedGuard(42, tok)
	if !w.beginDetachedCommit(42, tok, &committing, context.Background()) {
		t.Fatal("beginDetachedCommit failed on a tracked entry")
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.extendLeaseLoop(ctx, 42, tok, done, &committing) }()
	// No tick can have fired yet (5s period): the loop must be quiet.
	time.Sleep(200 * time.Millisecond)
	if n := store.extendCount(); n != 0 {
		cancel()
		close(done)
		wg.Wait()
		t.Fatalf("ExtendLease calls=%d before cancel, want 0", n)
	}
	cancel()
	// The mode switch must renew immediately with a detached (live)
	// context, far before the next 5s tick.
	deadline := time.Now().Add(2 * time.Second)
	for store.extendCount() == 0 {
		if time.Now().After(deadline) {
			close(done)
			wg.Wait()
			t.Fatal("no detached ExtendLease within 2s of cancel (next tick is 5s away; a peer would reclaim first)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	store.mu.Lock()
	c := store.calls[0]
	store.mu.Unlock()
	if c.taskID != 42 {
		t.Fatalf("renewed task %d, want 42", c.taskID)
	}
	if !c.live {
		t.Fatal("detached renewal used a canceled context")
	}
	close(done)
	wg.Wait()
}

// oneShotActivityBackend returns one canned activity task, then nothing.
type oneShotActivityBackend struct {
	backend.Backend
	task    backend.Task
	claimed atomic.Bool
}

func (b *oneShotActivityBackend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	if req.Kind != "activity" {
		return b.Backend.ClaimTasks(ctx, req)
	}
	if b.claimed.CompareAndSwap(false, true) {
		return []backend.Task{b.task}, nil
	}
	return nil, nil
}

// TestTickActivitiesRunsClaimInCapturedGeneration is a regression test for
// the restart race: Shutdown's grace expiry falls between track and
// goroutine start, Shutdown releases the claim, and the worker restarts
// with a new live execution context. A delayed goroutine must run the
// claim under the captured (canceled) generation context and abort —
// fetching the new run's live context would execute an already-released
// task whose side effects fencing cannot undo.
func TestTickActivitiesRunsClaimInCapturedGeneration(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	task := backend.Task{ID: 1, Kind: "activity", Queue: "default", InstanceID: "i1", Name: "stale", Seq: 1, Attempt: 1}
	store := &oneShotActivityBackend{Backend: mem, task: task}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  time.Hour,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	proceed := make(chan struct{})
	var calls atomic.Int32
	RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-proceed:
			calls.Add(1)
			return "x", nil
		case <-actCtx.Done():
			return "", actCtx.Err()
		}
	}, WithName("stale"))

	oldCtx, oldCancel := context.WithCancel(context.Background())
	w.actMu.Lock()
	w.execCtx = oldCtx
	w.execCancel = oldCancel
	w.stopping = false
	w.actMu.Unlock()

	w.tickActivities(context.Background())
	// The dispatched goroutine is queued but has not run: this goroutine
	// never blocked since the spawn (GOMAXPROCS=1).

	// Simulate Shutdown grace expiry + lease release + worker restart,
	// synchronously (no blocking calls, so the queued goroutine still has
	// not run).
	oldCancel()
	w.releaseInFlight(context.Background())
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	w.actMu.Lock()
	w.execCtx = newCtx
	w.execCancel = newCancel
	w.stopping = false
	w.actMu.Unlock()

	close(proceed)
	// Let the delayed goroutine run: it must abort on the captured
	// canceled context before reaching the activity.
	time.Sleep(500 * time.Millisecond)

	if n := calls.Load(); n != 0 {
		t.Fatalf("stale activity executed %d times under the new run's context, want 0 (released claim must abort)", n)
	}
	// Drain the goroutine before finishing.
	waitDone := make(chan struct{})
	go func() { w.actWg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatched goroutine did not finish")
	}
}
