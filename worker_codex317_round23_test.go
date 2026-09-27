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
	"github.com/hirokazumiyaji/tasuki/journal"
)

// blockAdvBackend pins CommitAdvancement(s) on a test-controlled gate so
// the test can hold a workflow flush past the lease while a peer tries to
// reclaim the committing task.
type blockAdvBackend struct {
	backend.Backend
	entered     chan struct{}
	enteredOnce atomic.Bool
	release     chan struct{}
}

func (b *blockAdvBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	if b.enteredOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	<-b.release
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *blockAdvBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if b.enteredOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	<-b.release
	if ctx.Err() != nil {
		return ctx.Err()
	}
	batcher, ok := b.Backend.(backend.AdvancementBatcher)
	if !ok {
		return backend.ErrConflict
	}
	return batcher.CommitAdvancements(ctx, advs)
}

// round23SetupWorkflowClaim creates a workflow instance, claims its
// workflow task, and tracks the claim on w from claimStart (the
// conservative pre-claim instant production sites use). It returns the
// claimed task, its in-flight token, and the workflow head for building
// the pending advancement.
func round23SetupWorkflowClaim(t *testing.T, ctx context.Context, mem *memory.Backend, w *Worker, instanceID string, lease time.Duration, claimStart time.Time) (backend.Task, claimToken, *backend.WorkflowState) {
	t.Helper()
	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: instanceID, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: lease, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(tasks))
	}
	st, err := mem.LoadWorkflow(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	return tasks[0], w.trackTaskAt(tasks[0], claimStart), st
}

// round23Pending builds the production-shaped pending commit for a
// claimed task: one timer event advancing the head sequence, carrying
// the claiming invocation's token.
func round23Pending(task backend.Task, st *backend.WorkflowState, tok claimToken) pendingWorkflowCommit {
	return pendingWorkflowCommit{
		instanceID:  task.InstanceID,
		baseJournal: st.Journal,
		task:        task,
		adv: backend.Advancement{
			InstanceID:  task.InstanceID,
			TaskID:      task.ID,
			ExpectedSeq: st.NextSeq,
			NewEvents: []journal.Event{
				{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-02T00:00:00Z"}`)},
			},
		},
		tok:    tok,
		hasTok: true,
	}
}

func round23PeerClaim(t *testing.T, ctx context.Context, mem *memory.Backend, workerID string) []backend.Task {
	t.Helper()
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: workerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return peer
}

// TestWorker_Round23_StaleFlushSkipsExpiredLease is the regression test
// for round-23 P2 (preserve workflow lease ownership through detached
// flushes), half 1: the freshness gate. A turn that consumed its whole
// lease lets a peer reclaim before the flush; the stale advancement must
// not reach the store even though the in-flight transfer would still
// "win" — backends validate by task ID + sequence alone, so the commit
// would delete the peer's active task after duplicate execution.
//
// Layout: 150ms lease, wall sleep + store-clock advance past expiry,
// peer reclaim, then flush the stale pending. Without the fix the flush
// transfers (entry present, token matches — no expiry check) and commits:
// the head advances and the peer's task row is deleted.
func TestWorker_Round23_StaleFlushSkipsExpiredLease(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: 150 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 150 * time.Millisecond
	task, tok, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round23-stale-1", lease, time.Now())
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, tok)

	// The turn consumes the whole lease: wall expiry passes (local
	// estimate) and the store lease lapses (peer-reclaimable).
	time.Sleep(250 * time.Millisecond)
	mem.SetNow(t0.Add(time.Second))
	peer := round23PeerClaim(t, ctx, mem, "peer")
	if len(peer) != 1 || peer[0].ID != task.ID {
		t.Fatalf("peer claim = %v, want the reclaimed task %d (setup must let the peer reclaim past expiry)", peer, task.ID)
	}

	w.flushWorkflowCommits(ctx, []pendingWorkflowCommit{pending})

	stAfter, err := mem.LoadWorkflow(ctx, "round23-stale-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore {
		t.Fatalf("head nextSeq = %d, want %d (stale advancement must not apply after a peer reclaim)", stAfter.NextSeq, nextBefore)
	}
	// The peer's lease is intact: past its minute lease it reclaims the
	// same task instead of finding a deleted row.
	mem.SetNow(t0.Add(time.Second).Add(2 * time.Minute))
	again := round23PeerClaim(t, ctx, mem, "w3")
	if len(again) != 1 || again[0].ID != task.ID {
		t.Fatalf("post-flush claim = %v, want peer task %d (stale flush must not delete the peer's task)", again, task.ID)
	}
}

// TestWorker_Round23_FlushKeepsLeaseAliveThroughBlockedCommit is the
// regression test for round-23 P2, half 2: the flush cover. A
// CommitAdvancement blocked past the remaining lease must not let a peer
// reclaim mid-flush: renewal stays alive until the flush returns, so the
// peer never observes a visible task and the commit lands on our own
// lease.
//
// Layout: 400ms lease, commit blocked ~1.2s while the store clock
// advances 100ms per 100ms wall (faster than the half-lease cover tick
// only if cover runs: each renewal pushes visible_at 400ms of store
// time forward). A peer claims after every advance. Without the fix
// nothing renews during the flush: the task goes visible after ~400ms
// of store time, the peer reclaims it, and the unblocked commit deletes
// the peer's task.
func TestWorker_Round23_FlushKeepsLeaseAliveThroughBlockedCommit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mem := memory.New()
	mem.SetNow(t0)
	store := &blockAdvBackend{
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
	task, tok, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round23-cover-1", lease, time.Now())
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, tok)

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(ctx, []pendingWorkflowCommit{pending})
	}()

	select {
	case <-store.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("commit did not start (flush never reached the store)")
	}
	// Hold the commit ~1.2s (3x the lease) while the store clock runs
	// and a peer polls after every advance.
	memNow := t0
	peerClaims := 0
	for i := 0; i < 12; i++ {
		time.Sleep(100 * time.Millisecond)
		memNow = memNow.Add(100 * time.Millisecond)
		mem.SetNow(memNow)
		if got := round23PeerClaim(t, ctx, mem, "peer"); len(got) != 0 {
			peerClaims++
		}
	}
	close(store.release)
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after the commit gate released")
	}

	if peerClaims != 0 {
		t.Fatalf("peer claimed the committing task %d time(s), want 0 (flush cover must keep the lease live through a blocked commit)", peerClaims)
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round23-cover-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore+1 {
		t.Fatalf("head nextSeq = %d, want %d (covered commit must still apply to our own lease)", stAfter.NextSeq, nextBefore+1)
	}
	// The task row is gone (committed), not reclaimed: nothing claimable.
	mem.SetNow(memNow.Add(time.Hour))
	if got := round23PeerClaim(t, ctx, mem, "w3"); len(got) != 0 {
		t.Fatalf("post-flush claim = %v, want empty (committed task must be deleted)", got)
	}
}
