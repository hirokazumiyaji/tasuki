package worker

import (
	"context"
	"sync"
	"time"
)

type workflowActor struct {
	mu       sync.Mutex
	lastUsed time.Time
}

func (a *workflowActor) dispatch(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn()
}

// lockCtx acquires the actor lock abandonably: it reports false when ctx
// ends first instead of blocking forever. A turn queued on the actor
// while its execution context is canceled (Shutdown, parent cancel)
// must stop waiting so tick-level joins complete and the queued claim
// is released promptly instead of running late onto a moved-on lease.
func (a *workflowActor) lockCtx(ctx context.Context) bool {
	if a.mu.TryLock() {
		return true
	}
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			if a.mu.TryLock() {
				return true
			}
		}
	}
}

func (w *Worker) actorFor(instanceID string) *workflowActor {
	w.instMu.Lock()
	defer w.instMu.Unlock()
	e, ok := w.instLock[instanceID]
	if !ok {
		e = &workflowActor{}
		w.instLock[instanceID] = e
	}
	e.lastUsed = time.Now()
	return e
}

// dispatchWorkflow runs fn holding the per-instance actor lock, waiting
// for it abandonably. It reports false when ctx ends before the turn
// acquires the actor: the caller must then stop the claim's renewal and
// release the lease instead of running the turn late.
//
// After acquiring the lock it re-checks that the actor is still the
// instance's current one: evictIdleInstanceLocks may have replaced the
// map entry while this turn queued on a stale object (eviction only
// proceeds on an unlocked actor, but the handoff between release and
// acquire admits it), and running on the stale lock would execute
// concurrently with turns on the fresh one. On a mismatch it retries
// on the current actor, still bounded by ctx. Each lookup refreshes
// lastUsed, so the retry always terminates.
func (w *Worker) dispatchWorkflow(ctx context.Context, instanceID string, fn func()) bool {
	for {
		actor := w.actorFor(instanceID)
		if !actor.lockCtx(ctx) {
			return false
		}
		if cur := w.actorFor(instanceID); cur == actor {
			fn()
			actor.mu.Unlock()
			return true
		}
		actor.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		default:
		}
	}
}

func (w *Worker) evictIdleInstanceLocks(now time.Time) {
	ttl := w.opts.IdleInstanceLockTTL
	w.instMu.Lock()
	defer w.instMu.Unlock()
	for id, e := range w.instLock {
		if now.Sub(e.lastUsed) < ttl {
			continue
		}
		if !e.mu.TryLock() {
			continue
		}
		delete(w.instLock, id)
		e.mu.Unlock()
	}
}
