package tasuki

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// round24BatchBlockBackend blocks the batched advancement commit on a
// test-controlled gate while honoring the commit context, so the test can
// trip a member's cover guard mid-block exactly as a failed cover renewal
// would (see renewOnceDetached).
type round24BatchBlockBackend struct {
	backend.Backend
	entered     chan struct{}
	enteredOnce atomic.Bool
	release     chan struct{}
	batchCalls  atomic.Int32
}

func (b *round24BatchBlockBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	b.batchCalls.Add(1)
	if b.enteredOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	batcher, ok := b.Backend.(backend.AdvancementBatcher)
	if !ok {
		return backend.ErrConflict
	}
	return batcher.CommitAdvancements(ctx, advs)
}

// round24GateExtendBackend pins the first ExtendLease on a
// test-controlled gate while recording whether the result commit reached
// the store, so the test can hold the initial cover renewal open and
// observe whether the flush admits the write before continuity is proven.
type round24GateExtendBackend struct {
	backend.Backend
	extendEntered chan struct{}
	extendOnce    atomic.Bool
	extendRelease chan struct{}
	commitEntered atomic.Bool
}

func (b *round24GateExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	if b.extendOnce.CompareAndSwap(false, true) {
		close(b.extendEntered)
	}
	select {
	case <-b.extendRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.Backend.ExtendLease(ctx, task, d)
}

func (b *round24GateExtendBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.commitEntered.Store(true)
	return b.Backend.CommitAdvancement(ctx, adv)
}

// TestWorker_Round24_BatchAbortsOnMidBlockCoverLoss is the regression test
// for round-24 P1 (fence the batch advancement write). The batch fast path
// bypasses the per-item guardedDetachedCommit: without an aggregate gate a
// cover renewal that fails mid-flush only removes its own guard while the
// blocked batch still commits every member by task ID + sequence —
// deleting a peer's reclaimed task.
//
// Layout: two gated pendings, batch commit blocked in the backend, then a
// cover-loss observation for member 1 mid-block (the trip a failed cover
// renewal performs). The batch must abort via the stashed batch cancel and
// fall back per item: member 1's missing guard skips its write while
// member 2 still commits. Without the fix nothing cancels the batch — it
// applies both advancements after the release.
func TestWorker_Round24_BatchAbortsOnMidBlockCoverLoss(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	store := &round24BatchBlockBackend{
		Backend: mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 400 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 400 * time.Millisecond
	claimStart := time.Now()
	task1, tok1, st1 := round23SetupWorkflowClaim(t, ctx, mem, w, "round24-batch-1", lease, claimStart)
	task2, tok2, st2 := round23SetupWorkflowClaim(t, ctx, mem, w, "round24-batch-2", lease, claimStart)
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
	case <-store.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("batch commit did not start (flush never reached the store)")
	}
	// A cover renewal for member 1 observes lease loss mid-block: trip
	// its guard, exactly as renewOnceDetached does on failure. The batch
	// cancel stashed in every member's guard must abort the submission.
	w.tripDetachedGuard(task1.ID, tok1)
	close(store.release)
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after the batch gate released")
	}

	if n := store.batchCalls.Load(); n != 1 {
		t.Fatalf("batch submissions = %d, want 1 (the fenced batch must still be attempted once)", n)
	}
	st1After, err := mem.LoadWorkflow(ctx, "round24-batch-1")
	if err != nil {
		t.Fatal(err)
	}
	if st1After.NextSeq != next1 {
		t.Fatalf("member-1 nextSeq = %d, want %d (cover loss mid-block must abort its write)", st1After.NextSeq, next1)
	}
	st2After, err := mem.LoadWorkflow(ctx, "round24-batch-2")
	if err != nil {
		t.Fatal(err)
	}
	if st2After.NextSeq != next2+1 {
		t.Fatalf("member-2 nextSeq = %d, want %d (the live member must still commit via fallback)", st2After.NextSeq, next2+1)
	}
	// Member 1's task row survives (never committed, never reclaimed by
	// us); member 2's is deleted by its commit.
	mem.SetNow(t0.Add(time.Hour))
	got := round23PeerClaim(t, ctx, mem, "peer")
	ids := map[int64]bool{}
	for _, task := range got {
		ids[task.ID] = true
	}
	if !ids[task1.ID] {
		t.Fatalf("post-flush claim = %v, want member-1 task %d still present (aborted batch must not delete it)", got, task1.ID)
	}
	if ids[task2.ID] {
		t.Fatalf("post-flush claim = %v, member-2 task %d must be deleted by its commit", got, task2.ID)
	}
	_ = tok2
}

// TestWorker_Round24_FlushWaitsForInitialCover is the regression test for
// round-24 P2 (await the initial workflow cover renewal). startFlushCover
// used to launch renewal goroutines and return immediately, so a slow
// CommitAdvancement could start before the first renewal even ran: a
// scheduling delay past expiry lets a peer reclaim while the late ID-only
// renewal only observes the loss after the stale write.
//
// Layout: a backend whose first ExtendLease blocks on a gate. The flush
// must not reach the store while the initial renewal is held open —
// without the fix the commit starts immediately and the test observes the
// store op during the block.
func TestWorker_Round24_FlushWaitsForInitialCover(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	store := &round24GateExtendBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 400 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 400 * time.Millisecond
	task, _, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round24-cover-1", lease, time.Now())
	// Re-track so the pending carries the current in-flight token for
	// the freshness gate below.
	pending := round23Pending(task, st, w.trackTaskAt(task, time.Now()))

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(ctx, []pendingWorkflowCommit{pending})
	}()

	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("initial cover renewal did not start")
	}
	// Hold the initial renewal open past the point where an async cover
	// would already have admitted the write. The commit must not reach
	// the store until continuity is proven.
	time.Sleep(300 * time.Millisecond)
	if store.commitEntered.Load() {
		t.Fatal("commit reached the store while the initial cover renewal was still blocked (flush must await the first renewal before admitting writes)")
	}
	close(store.extendRelease)
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after the cover gate released")
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round24-cover-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != st.NextSeq+1 {
		t.Fatalf("head nextSeq = %d, want %d (covered commit must still apply once the initial renewal succeeds)", stAfter.NextSeq, st.NextSeq+1)
	}
}
