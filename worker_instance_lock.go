package tasuki

import (
	"sync"
	"time"
)

type instanceLock struct {
	mu       sync.Mutex
	lastUsed time.Time
}

func (w *Worker) instanceMutex(instanceID string) *sync.Mutex {
	w.instMu.Lock()
	defer w.instMu.Unlock()
	e, ok := w.instLock[instanceID]
	if !ok {
		e = &instanceLock{}
		w.instLock[instanceID] = e
	}
	e.lastUsed = time.Now()
	return &e.mu
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
