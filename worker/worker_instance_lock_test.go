package worker

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestWorkflowActorDispatchesTurnsSequentially(t *testing.T) {
	var actor workflowActor
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	order := make(chan int, 2)

	go actor.dispatch(func() {
		close(firstStarted)
		<-releaseFirst
		order <- 1
	})
	<-firstStarted
	go actor.dispatch(func() { order <- 2 })

	close(releaseFirst)
	if got := <-order; got != 1 {
		t.Fatalf("first callback order = %d", got)
	}
	if got := <-order; got != 2 {
		t.Fatalf("second callback order = %d", got)
	}
}

func TestEvictIdleInstanceLocks(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{IdleInstanceLockTTL: time.Minute})
	_ = w.actorFor("a")
	_ = w.actorFor("b")

	held := w.actorFor("held")
	held.mu.Lock()
	defer held.mu.Unlock()

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
	_ = w.actorFor("fresh")
	w.evictIdleInstanceLocks(time.Now())
	w.instMu.Lock()
	defer w.instMu.Unlock()
	if _, ok := w.instLock["fresh"]; !ok {
		t.Fatal("recent lock should remain")
	}
}
