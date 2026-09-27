package tasuki

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// blockingLeaseBackend models a store whose ReleaseLease hangs until the
// caller's context ends (e.g. a wedged store or a saturated connection
// pool), so each release consumes its full timeout budget.
type blockingLeaseBackend struct {
	backend.Backend
	releases atomic.Int32
}

func (b *blockingLeaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

// TestWorker_Round17_CanceledTickReleasesShareOneBudget is the regression
// test for round-17 P2 (share one timeout across pending lease releases):
// a shutdown-canceled tick with several completed pendings released each
// lease with a FRESH ShutdownReleaseTimeout, so Shutdown cost up to
// len(pending)×timeout with a blocking backend even though the option
// documents a single bound. With the fix the whole dispose batch shares
// one deadline (~1×timeout): the first blocked release consumes the
// budget and the rest fail fast, and unreleased leases expire naturally
// for peer reclaim.
//
// Layout: 4 FAST turns complete (pending) while 1 SLOW turn holds
// wg.Wait() open; the tick context is canceled in that window, then the
// slow turn is released and abandons (its single release takes ~1×budget
// in both versions). The timed section covers the slow abandon plus the
// 4-pending dispose: old code pays ~1+4 budgets, fixed code ~1+1.
func TestWorker_Round17_CanceledTickReleasesShareOneBudget(t *testing.T) {
	const (
		fastTurns = 4
		budget    = 300 * time.Millisecond
		// Old code needs slow(1×) + 4×budget ≈ 1500ms minimum (each
		// blocked release consumes its full context timeout); fixed code
		// needs ≈ 2×budget. The bound sits between them with headroom
		// for slow CI on the fixed side.
		upperBound = 1200 * time.Millisecond
	)

	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &blockingLeaseBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:           time.Hour, // tick driven manually below
		LeaseDuration:          30 * time.Second,
		WorkerID:               "w1",
		ClaimLimit:             10,
		WorkflowConcurrency:    10,
		ActivityConcurrency:    10,
		ShutdownReleaseTimeout: budget,
	})

	fastGate := make(chan struct{})
	slowGate := make(chan struct{})
	var fastEntered, fastDone, slowEntered atomic.Int32
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		fastEntered.Add(1)
		<-fastGate
		fastDone.Add(1)
		return "fast-ok", nil
	}, WithName("FAST"))
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		slowEntered.Add(1)
		<-slowGate
		return "slow-ok", nil
	}, WithName("SLOW"))

	ctx := context.Background()
	c := NewClient(store)
	for i := 0; i < fastTurns; i++ {
		if _, err := Start(ctx, c, "FAST", struct{}{}, WithID(fmt.Sprintf("round17-batch-fast-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Start(ctx, c, "SLOW", struct{}{}, WithID("round17-batch-slow")); err != nil {
		t.Fatal(err)
	}

	tickCtx, cancel := context.WithCancel(context.Background())
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()

	// Wait until all 5 turns are claimed and parked in their workflow
	// functions, then let the fast turns complete while the slow turn
	// keeps wg.Wait() open.
	waitFor(t, 10*time.Second, "fast and slow turns to start", func() bool {
		return fastEntered.Load() == fastTurns && slowEntered.Load() == 1
	})
	close(fastGate)
	waitFor(t, 10*time.Second, "fast turns to finish", func() bool {
		return fastDone.Load() == fastTurns
	})
	// Let the fast post-run checks build the pending batch while the
	// tick context is still live (pending built ⇒ dispose path, not
	// abandon, once canceled below).
	time.Sleep(200 * time.Millisecond)

	// Shutdown cancels the tick with the pendings already completed.
	cancel()
	// Release the slow turn; the timed section below covers its abandon
	// release plus the pending-batch dispose.
	close(slowGate)
	start := time.Now()
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tickWorkflows did not return (dispose must be bounded by one shared budget)")
	}
	elapsed := time.Since(start)
	if elapsed > upperBound {
		t.Fatalf("canceled tick dispose took %v, want <%v (N pendings must share one ShutdownReleaseTimeout, not one each)", elapsed, upperBound)
	}

	// Every turn left the in-flight set exactly once: claimed for
	// release (released) or handed to expiry (reclaimable). Nothing may
	// still be tracked.
	w.mu.Lock()
	left := len(w.inFlight)
	w.mu.Unlock()
	if left != 0 {
		t.Fatalf("inFlight=%d after dispose, want 0 (every pending must be released-or-expired)", left)
	}
	if n := store.releases.Load(); n < 2 {
		t.Fatalf("ReleaseLease attempts=%d, want >=2 (slow abandon + at least one batch attempt)", n)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
