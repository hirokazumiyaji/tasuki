package tasuki

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestEvictIdleInstanceLocks(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{IdleInstanceLockTTL: time.Minute})
	_ = w.instanceMutex("a")
	_ = w.instanceMutex("b")

	held := w.instanceMutex("held")
	held.Lock()
	defer held.Unlock()

	w.instMu.Lock()
	w.instLock["a"].lastUsed = time.Now().Add(-2 * time.Minute)
	w.instLock["b"].lastUsed = time.Now().Add(-2 * time.Minute)
	w.instLock["held"].lastUsed = time.Now().Add(-2 * time.Minute)
	w.instMu.Unlock()

	w.evictIdleInstanceLocks(time.Now())

	w.instMu.Lock()
	defer w.instMu.Unlock()
	if _, ok := w.instLock["a"]; ok {
		t.Fatal("expected a evicted")
	}
	if _, ok := w.instLock["b"]; ok {
		t.Fatal("expected b evicted")
	}
	if _, ok := w.instLock["held"]; !ok {
		t.Fatal("held lock must not be evicted while locked")
	}
}

func TestEvictIdleInstanceLocks_RecentKept(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{IdleInstanceLockTTL: time.Minute})
	_ = w.instanceMutex("fresh")
	w.evictIdleInstanceLocks(time.Now())
	w.instMu.Lock()
	defer w.instMu.Unlock()
	if _, ok := w.instLock["fresh"]; !ok {
		t.Fatal("recent lock should remain")
	}
}
