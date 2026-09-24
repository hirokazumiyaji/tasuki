package tasuki

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestWorker_Round27_SkewedVisibleAtKeepsLocalBaseline is the regression
// test for round-27 (keep store timestamps out of local lease
// arithmetic). Task.VisibleAt is stamped by the STORE clock (postgres
// now()) while the renewal baseline is measured on the WORKER clock, so
// the two are incomparable under skew: with the store clock behind, a
// short lease plus skew puts the VisibleAt-derived start before the
// local abandonment deadline, and the pre-fix min() adopts it — the
// first renewal then instantly cancels every turn though the DB lease is
// still valid.
//
// Layout: 600ms lease (margin 150ms, abandonment deadline
// claimBase+450ms), store down, claimBase = now, VisibleAt = now+100ms —
// still ahead of now (the DB lease is valid) but 500ms behind the local
// clock, so its derived start (now-500ms) sits past the local deadline.
// The fix bases the window on claimBase alone: the first tick fires at
// ~300ms and the loop retries in-window (3 ExtendLease attempts,
// abandoning at ~450ms). Without the fix the baseline is now-500ms, the
// deadline already passed, and the loop abandons after a single attempt
// in ~10ms.
func TestWorker_Round27_SkewedVisibleAtKeepsLocalBaseline(t *testing.T) {
	ctx := context.Background()
	const lease = 600 * time.Millisecond
	store := &round23FailBackend{}
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	start := time.Now()
	task := backend.Task{ID: 7, VisibleAt: start.Add(100 * time.Millisecond)}
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		w.extendLeaseLoop(ctx, task, done, func() { lost.Add(1) }, start)
	}()
	select {
	case <-loopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("renewal loop did not abandon a skewed claim")
	}
	elapsed := time.Since(start)
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1", n)
	}
	if n := store.calls.Load(); n != 3 {
		t.Fatalf("ExtendLease calls = %d, want 3 (first tick + 2 in-window retries; an instant abandon runs 1)", n)
	}
	// Abandonment belongs at the local claimBase+450ms deadline. 350ms
	// splits it from the ~10ms instant abandon with margin for
	// timer/scheduling jitter on either side.
	if elapsed < 350*time.Millisecond {
		t.Fatalf("abandoned after %v, want >= 350ms (a skewed-but-valid VisibleAt must not cancel the turn instantly)", elapsed)
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("abandoned after %v, want well before 5s", elapsed)
	}
}
