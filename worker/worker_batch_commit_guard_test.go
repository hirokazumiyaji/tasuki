package worker

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/observability"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// round26GateBatchBackend holds a batch advancement submission on a
// test-controlled gate while IGNORING the passed context, simulating a
// store that applies a stale batch after its cancellation was requested.
// Single (per-item) commits are counted and delegated; ExtendLease is
// delegated unwrapped so initial cover renewals prove continuity promptly.
type round26GateBatchBackend struct {
	backend.Backend
	batchEntered chan struct{}
	batchOnce    atomic.Bool
	batchRelease chan struct{}
	batchCalls   atomic.Int32
	singleCalls  atomic.Int32
}

func (b *round26GateBatchBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.singleCalls.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *round26GateBatchBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if b.batchOnce.CompareAndSwap(false, true) {
		close(b.batchEntered)
	}
	// Deliberately ignore ctx: apply the stale batch after cancel.
	<-b.batchRelease
	b.batchCalls.Add(1)
	batcher, ok := b.Backend.(backend.AdvancementBatcher)
	if !ok {
		return backend.ErrConflict
	}
	return batcher.CommitAdvancements(ctx, advs)
}

// TestWorker_BatchSuccessRecheckedAgainstMidCallGuardLoss is the
// regression test for round-26 P1b (fence batch success against mid-call
// guard loss). When a cover renewal trips a member's guard while
// CommitAdvancements is blocked, the trip only cancels the shared batch
// context; a batcher that ignores cancellation still applies the stale
// batch and returns nil. Without a post-op guard recheck the caller treats
// every member as successfully committed.
//
// Layout: two gated pendings with proven initial cover; the batch blocks
// mid-call; the test trips the second member's guard (simulating its cover
// renewal observing lease loss); the batch is released and applies both
// advancements. The fix must NOT mark the tripped member committed (no
// sticky for it) while the live member keeps its result, and no per-item
// store op may run for either (the live member already committed via the
// batch; the lost member's fallback re-gate skips its write). Without the
// fix both members get sticky applied as committed.
func TestWorker_BatchSuccessRecheckedAgainstMidCallGuardLoss(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	store := &round26GateBatchBackend{
		Backend:      mem,
		batchEntered: make(chan struct{}),
		batchRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 400 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 400 * time.Millisecond
	claimStart := time.Now()
	task1, tok1, st1 := round23SetupWorkflowClaim(t, ctx, mem, w, "round26-batch-1", lease, claimStart)
	task2, tok2, st2 := round23SetupWorkflowClaim(t, ctx, mem, w, "round26-batch-2", lease, claimStart)
	next1, next2 := st1.NextSeq, st2.NextSeq
	pending := []pendingWorkflowCommit{
		round23Pending(task1, st1, tok1),
		round23Pending(task2, st2, tok2),
	}

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(ctx, pending)
	}()
	select {
	case <-store.batchEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("batch submission did not start")
	}
	// Member 2 loses its lease mid-call; member 1 stays live.
	w.tripDetachedGuard(task2.ID, tok2)
	close(store.batchRelease)
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after the batch applied")
	}

	if n := store.batchCalls.Load(); n != 1 {
		t.Fatalf("CommitAdvancements calls = %d, want 1 (batch ran once)", n)
	}
	if n := store.singleCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancement calls = %d, want 0 (live member already committed via the batch; lost member's fallback must skip its write)", n)
	}
	// The stale batch applied both advancements — the post-op recheck
	// cannot undo the write, it only fences the aftermath. Both heads
	// advanced; the fix's observable is who is MARKED committed.
	st1After, err := mem.LoadWorkflow(ctx, "round26-batch-1")
	if err != nil {
		t.Fatal(err)
	}
	st2After, err := mem.LoadWorkflow(ctx, "round26-batch-2")
	if err != nil {
		t.Fatal(err)
	}
	if d := st1After.NextSeq - next1; d != 1 {
		t.Fatalf("member 1 advancement delta = %d, want 1 (live member committed)", d)
	}
	if d := st2After.NextSeq - next2; d != 1 {
		t.Fatalf("member 2 advancement delta = %d, want 1 (stale batch applied before the recheck)", d)
	}
	w.stickyMu.Lock()
	_, ok1 := w.sticky["round26-batch-1"]
	_, ok2 := w.sticky["round26-batch-2"]
	w.stickyMu.Unlock()
	if !ok1 {
		t.Fatal("live member has no sticky entry (committed members must keep their result)")
	}
	if ok2 {
		t.Fatal("tripped member marked committed (mid-call lease loss must mark the member failed, not committed)")
	}
	w.detMu.Lock()
	leftover := len(w.detGuard)
	w.detMu.Unlock()
	if leftover != 0 {
		t.Fatalf("detached guards left = %d, want 0 (committed guards ended, lost guards dropped)", leftover)
	}
}

// round26BlockExtendBackend blocks ExtendLease on a test-controlled gate
// while IGNORING the passed context, then delegates to the wrapped
// backend so the late call SUCCEEDS (lands) instead of failing like the
// round-25 outage simulation. Single commits are counted and delegated.
type round26BlockExtendBackend struct {
	backend.Backend
	extendEntered chan struct{}
	extendOnce    atomic.Bool
	extendRelease chan struct{}
	commitCalls   atomic.Int32
}

func (b *round26BlockExtendBackend) ExtendLease(_ context.Context, task backend.Task, d time.Duration) error {
	if b.extendOnce.CompareAndSwap(false, true) {
		close(b.extendEntered)
	}
	// Deliberately ignore ctx: block until the test releases, then land.
	<-b.extendRelease
	return b.Backend.ExtendLease(context.Background(), task, d)
}

func (b *round26BlockExtendBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.commitCalls.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

// TestWorker_StaleInitialRenewalCompensated is the regression test
// for round-26 P1a (fence a timed-out initial renewal that survives guard
// teardown). When CommitTimeout expires while the initial ExtendLease is
// blocked in a context-ignoring backend, the bounded Phase 1 join trips
// the guard and lets the flush return with the backend call still in
// flight. Dropping its result is not enough: the ID-only write may still
// land afterwards and re-hide the task for a full lease with no renewal
// loop left to own it.
//
// Layout (real store clock): 1s lease, 200ms commit bound, ExtendLease
// blocked ignoring ctx. The flush must return around the commit bound with
// no store write (continuity never proven). Past lease expiry the test
// releases the renewal so it lands successfully; the fix issues a
// token-fenced compensation release (the local lease already expired), so
// a peer can reclaim the task promptly. Without the fix the late landing
// hides the task for another full lease and the peer claim finds nothing.
func TestWorker_StaleInitialRenewalCompensated(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	store := &round26BlockExtendBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: time.Second,
		WorkerID:      "w1",
		Metrics:       m,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = time.Second
	claimStart := time.Now()
	task, _, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round26-stale-1", lease, claimStart)
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, w.trackTaskAt(task, time.Now()))

	commitCtx, commitCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer commitCancel()
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(commitCtx, []pendingWorkflowCommit{pending})
	}()
	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("initial cover renewal did not start")
	}
	select {
	case <-flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not return within 5s while ExtendLease stalled ignoring ctx")
	}
	if n := store.commitCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancement calls = %d, want 0 (unproven continuity must skip the store op)", n)
	}

	// Wait past the local lease expiry, then let the stale renewal land.
	expiry := claimStart.Add(lease)
	if wait := time.Until(expiry.Add(200 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	close(store.extendRelease)
	// The compensation runs synchronously in the renewal goroutine right
	// after the backend call returns; allow scheduling slack, far below
	// the full-lease hiding the late landing causes without the fix.
	time.Sleep(300 * time.Millisecond)

	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatal("peer could not reclaim promptly after the stale renewal landed (late ID-only write re-hid the task; want a fenced compensation release)")
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round26-stale-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore {
		t.Fatalf("head nextSeq = %d, want %d (stalled cover must not apply)", stAfter.NextSeq, nextBefore)
	}
	if n := storeErrorTotal(t, ctx, reader, "commit_workflow"); n != 0 {
		t.Fatalf("store_errors{op=commit_workflow} = %d, want 0 (skipped fenced commit is not a store failure)", n)
	}
	if n := storeErrorTotal(t, ctx, reader, "extend_lease"); n != 0 {
		t.Fatalf("store_errors{op=extend_lease} = %d, want 0 (dropped stale renewal is fencing, not a store failure)", n)
	}
}
