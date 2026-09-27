package worker

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestExpectedNextSeq(t *testing.T) {
	if got := expectedNextSeq(nil); got != 1 {
		t.Fatalf("empty=%d", got)
	}
	if got := expectedNextSeq([]journal.Event{{Seq: 3}, {Seq: 7}}); got != 8 {
		t.Fatalf("got %d", got)
	}
}

func TestEvictIdleSticky(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{StickyJournalTTL: time.Minute})
	w.setSticky("a", []journal.Event{{Seq: 1}}, 2)
	w.setSticky("b", []journal.Event{{Seq: 1}}, 2)
	w.setSticky("fresh", []journal.Event{{Seq: 1}}, 2)

	w.stickyMu.Lock()
	e := w.sticky["a"]
	e.lastUsed = time.Now().Add(-2 * time.Minute)
	w.sticky["a"] = e
	e = w.sticky["b"]
	e.lastUsed = time.Now().Add(-2 * time.Minute)
	w.sticky["b"] = e
	w.stickyMu.Unlock()

	w.evictIdleSticky(time.Now())

	if _, ok := w.stickyGet("a"); ok {
		t.Fatal("expected a evicted")
	}
	if _, ok := w.stickyGet("b"); ok {
		t.Fatal("expected b evicted")
	}
	if _, ok := w.stickyGet("fresh"); !ok {
		t.Fatal("recent sticky should remain")
	}
}

func TestEvictIdleSticky_RecentKept(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{StickyJournalTTL: time.Minute})
	w.setSticky("fresh", []journal.Event{{Seq: 1}}, 2)
	w.evictIdleSticky(time.Now())
	if _, ok := w.stickyGet("fresh"); !ok {
		t.Fatal("recent sticky should remain")
	}
}
