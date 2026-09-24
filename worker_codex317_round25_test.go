package tasuki

import (
	"context"
	"errors"
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

// round25IgnoreExtendBackend blocks ExtendLease forever while IGNORING the
// passed context, simulating an outage where the store hangs without
// observing cancellation. The block is released by closing extendRelease
// (test cleanup) so leaked renewal goroutines can exit; the flush must
// already have returned by then via its commit-context bound.
type round25IgnoreExtendBackend struct {
	backend.Backend
	extendEntered chan struct{}
	extendOnce    atomic.Bool
	extendRelease chan struct{}
	commitCalls   atomic.Int32
	batchCalls    atomic.Int32
}

func (b *round25IgnoreExtendBackend) ExtendLease(_ context.Context, _ int64, _ time.Duration) error {
	if b.extendOnce.CompareAndSwap(false, true) {
		close(b.extendEntered)
	}
	// Deliberately ignore ctx: block until the test releases.
	<-b.extendRelease
	return errors.New("round25 released")
}

func (b *round25IgnoreExtendBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.commitCalls.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *round25IgnoreExtendBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	b.batchCalls.Add(1)
	batcher, ok := b.Backend.(backend.AdvancementBatcher)
	if !ok {
		return backend.ErrConflict
	}
	return batcher.CommitAdvancements(ctx, advs)
}

// TestWorker_Round25_FlushBoundedByCommitContext is the regression test for
// round-25 P1 (bound the initial workflow renewal by the commit context).
// startFlushCover used to derive cover renewals from a
// background-derived coverCtx with only a lease-duration timeout and join
// them with an unconditional Wait: an ExtendLease stalled in a
// context-ignoring backend held the flush past CommitTimeout, pinning the
// polling loop / PollOnce during an outage.
//
// Layout: 30s lease (renewal's own timeout is far away), flush commit ctx
// with a 300ms timeout, ExtendLease blocking while ignoring ctx. The flush
// must return around the commit bound without issuing any store write —
// continuity was never proven, so the gate skips. Without the fix the
// unconditional join blocks until the test releases the gate (the test
// fails its flushDone timeout).
func TestWorker_Round25_FlushBoundedByCommitContext(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	store := &round25IgnoreExtendBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	defer close(store.extendRelease)
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 30 * time.Second,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 30 * time.Second
	task, _, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round25-bound-1", lease, time.Now())
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, w.trackTaskAt(task, time.Now()))

	commitCtx, commitCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer commitCancel()

	start := time.Now()
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
		t.Fatal("flush did not return within 5s while ExtendLease stalled ignoring ctx (flush must be bounded by the commit context, not the lease timeout)")
	}
	elapsed := time.Since(start)
	// Commit bound is 300ms; allow generous scheduling slack but require
	// the flush to return far before the 30s lease-based renewal timeout.
	if elapsed > 5*time.Second {
		t.Fatalf("flush elapsed = %v, want < 5s (commit-context bound, not lease bound)", elapsed)
	}
	if n := store.commitCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancement calls = %d, want 0 (unproven continuity must skip the store op)", n)
	}
	if n := store.batchCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancements calls = %d, want 0 (single pending must not batch; stalled cover must skip the write)", n)
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round25-bound-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore {
		t.Fatalf("head nextSeq = %d, want %d (stalled cover must not apply)", stAfter.NextSeq, nextBefore)
	}
}

// round25FailFirstExtendBackend fails exactly the first ExtendLease call so
// one of two flush members loses its initial cover renewal deterministically
// (whichever parallel renewal runs first), tripping its guard before the
// aggregate batch gate runs.
type round25FailFirstExtendBackend struct {
	backend.Backend
	extendCalls atomic.Int32
	batchCalls  atomic.Int32
	singleCalls atomic.Int32
}

func (b *round25FailFirstExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	if b.extendCalls.Add(1) == 1 {
		return errors.New("round25 injected cover failure")
	}
	return b.Backend.ExtendLease(ctx, taskID, d)
}

func (b *round25FailFirstExtendBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.singleCalls.Add(1)
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *round25FailFirstExtendBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	b.batchCalls.Add(1)
	batcher, ok := b.Backend.(backend.AdvancementBatcher)
	if !ok {
		return backend.ErrConflict
	}
	return batcher.CommitAdvancements(ctx, advs)
}

// TestWorker_Round25_BatchLeaseLossSkipsStoreError is the regression test
// for round-25 P2 (exclude batch lease-loss rejections from store errors).
// When the initial cover renewal fails (or a guard expires pre-submission)
// guardedDetachedBatchCommit aborts the whole batch with errLeaseLost
// without calling CommitAdvancements — but the batch branch recorded a
// commit_workflow backend failure, raising false alerts. The per-item path
// already treats errLeaseLost as a debug-only skip (round-22 fix); the
// batch branch must mirror it.
//
// Layout: two gated pendings, first cover renewal fails. The batch gate
// must abort with errLeaseLost (batchCalls == 0), the fallback must still
// commit the live member per item, and no commit_workflow store error may
// be recorded. Without the fix commit_workflow == 1.
func TestWorker_Round25_BatchLeaseLossSkipsStoreError(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	store := &round25FailFirstExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 400 * time.Millisecond,
		WorkerID:      "w1",
		Metrics:       m,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 400 * time.Millisecond
	claimStart := time.Now()
	task1, tok1, st1 := round23SetupWorkflowClaim(t, ctx, mem, w, "round25-batch-1", lease, claimStart)
	task2, tok2, st2 := round23SetupWorkflowClaim(t, ctx, mem, w, "round25-batch-2", lease, claimStart)
	next1, next2 := st1.NextSeq, st2.NextSeq
	pending := []pendingWorkflowCommit{
		round23Pending(task1, st1, tok1),
		round23Pending(task2, st2, tok2),
	}

	w.flushWorkflowCommits(ctx, pending)

	if n := store.batchCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancements calls = %d, want 0 (pre-submission gate loss must issue no batch store op)", n)
	}
	if n := storeErrorTotal(t, ctx, reader, "commit_workflow"); n != 0 {
		t.Fatalf("store_errors{op=commit_workflow} = %d, want 0 (batch lease-loss fencing is not a store failure)", n)
	}
	// Exactly one member fell back and committed per item: one head
	// advances by one, the other is unchanged.
	st1After, err := mem.LoadWorkflow(ctx, "round25-batch-1")
	if err != nil {
		t.Fatal(err)
	}
	st2After, err := mem.LoadWorkflow(ctx, "round25-batch-2")
	if err != nil {
		t.Fatal(err)
	}
	adv1 := st1After.NextSeq - next1
	adv2 := st2After.NextSeq - next2
	if (adv1 == 1 && adv2 == 0) || (adv1 == 0 && adv2 == 1) {
		return
	}
	t.Fatalf("advancement deltas = (%d, %d), want exactly one of (1, 0) or (0, 1) (live member commits via fallback, lost member skips)", adv1, adv2)
	_ = task1
	_ = task2
	_ = tok1
	_ = tok2
}
