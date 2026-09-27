package tasuki

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

func (a *workflowActor) dispatchContext(ctx context.Context, fn func()) bool {
	locked := make(chan struct{})
	go func() {
		a.mu.Lock()
		close(locked)
	}()
	select {
	case <-ctx.Done():
		go func() {
			<-locked
			a.mu.Unlock()
		}()
		return false
	case <-locked:
		defer a.mu.Unlock()
		if ctx.Err() != nil {
			return false
		}
		fn()
		return true
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
