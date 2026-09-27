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
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// round26BlockRenewBackend blocks every ExtendLease on a test-controlled
// gate while IGNORING the passed context, simulating a store/network
// outage where the renewal call never returns. The block is released by
// closing extendRelease (test cleanup) so orphaned renewal goroutines can
// exit; the loop and stopRenewal must already have moved on by then via
// the abandonment deadline.
type round26BlockRenewBackend struct {
	backend.Backend
	extendEntered chan struct{}
	extendOnce    atomic.Bool
	extendRelease chan struct{}
}

func (b *round26BlockRenewBackend) ExtendLease(_ context.Context, _ backend.Task, _ time.Duration) error {
	if b.extendOnce.CompareAndSwap(false, true) {
		close(b.extendEntered)
	}
	// Deliberately ignore ctx: block until the test releases.
	<-b.extendRelease
	return nil
}

// TestWorker_Round26_BlockedRenewalSignalsLossAtDeadline is the regression
// test for round-26 P1 (bound renewal calls to the remaining lease
// window), ticker half. When ExtendLease blocks while the store is
// unhealthy, it must not hold the renewal loop on the long-lived tick
// context past the abandonment deadline: the turn would keep running side
// effects after a peer reclaims at expiry.
//
// Layout: 400ms lease (abandonment deadline ~300ms after the claim), first
// renewal attempt blocked ignoring ctx with the loop's done left open. The
// loop must signal lease loss and return around the deadline although the
// call never returns. Without the fix the loop stays inside the blocked
// call and the test fails its timeout.
func TestWorker_Round26_BlockedRenewalSignalsLossAtDeadline(t *testing.T) {
	ctx := context.Background()
	const lease = 400 * time.Millisecond
	store := &round26BlockRenewBackend{
		Backend:       memory.New(),
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	defer close(store.extendRelease)
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	loopDone := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(loopDone)
		w.extendLeaseLoop(ctx, backend.Task{ID: 7}, done, func() { lost.Add(1) }, start)
	}()
	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("renewal attempt did not start")
	}
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal loop did not signal lease loss at the abandonment deadline while ExtendLease stayed blocked")
	}
	elapsed := time.Since(start)
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1 (blocked renewal must abandon at the deadline, not hold the turn)", n)
	}
	// Deadline is ~300ms after the claim; 2s splits prompt abandonment
	// from hanging behind the blocked call with wide scheduling margin.
	if elapsed >= 2*time.Second {
		t.Fatalf("abandoned after %v, want well before 2s (loop must exit at the deadline, not with the call)", elapsed)
	}
}

// TestWorker_Round26_BlockedRetrySignalsLossAtDeadline pins the same bound
// on the retry path: after a fast transient failure, a retry attempt that
// blocks must still abandon at the deadline instead of holding
// retryRenewal (and the turn) past it.
func TestWorker_Round26_BlockedRetrySignalsLossAtDeadline(t *testing.T) {
	ctx := context.Background()
	const lease = 400 * time.Millisecond
	store := &round26BlockRenewBackend{
		Backend:       memory.New(),
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	defer close(store.extendRelease)
	w := round23TestWorker(store, lease)
	done := make(chan struct{})
	defer close(done)
	var lost atomic.Int32
	start := time.Now()
	type retryOut struct {
		at      time.Time
		renewed bool
	}
	outCh := make(chan retryOut, 1)
	go func() {
		at, renewed := w.retryRenewal(ctx, backend.Task{ID: 7}, done, lease, start, func() { lost.Add(1) })
		outCh <- retryOut{at: at, renewed: renewed}
	}()
	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("renewal retry attempt did not start")
	}
	select {
	case out := <-outCh:
		if out.renewed {
			t.Fatal("renewed = true, want false (blocked retry must abandon at the deadline)")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retryRenewal did not abandon at the deadline while the attempt stayed blocked")
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("lease-loss signals = %d, want 1 (blocked retry must signal loss at the deadline)", n)
	}
}

// TestWorker_Round26_StopRenewalBoundedByBlockedCall pins the second half
// of round-26 P1: stopRenewal must not hang forever behind a blocked
// renewal call. A 2s-lease turn runs 1200ms (past its first renewal tick,
// which blocks ignoring ctx) and completes; the tick's final stop must
// return via the abandoned loop, far below the stop's own lease-scale cap
// (which would give up only past ~3.2s) — and infinitely below the
// pre-fix unbounded wait. Without the fix the tick hangs until the test
// releases the block (cleanup only), failing the timeout.
func TestWorker_Round26_StopRenewalBoundedByBlockedCall(t *testing.T) {
	ctx := context.Background()
	const lease = 2 * time.Second
	mem := memory.New()
	store := &round26BlockRenewBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	defer close(store.extendRelease)
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       lease,
		WorkerID:            "w1",
		ClaimLimit:          1,
		WorkflowConcurrency: 1,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	// A turn outliving its first renewal tick; the renewal blocks, the
	// turn ignores it and completes.
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		time.Sleep(1200 * time.Millisecond)
		return "done", nil
	}, WithName("slow26WF"))
	c := NewClient(mem)
	h, err := Start(ctx, c, "slow26WF", struct{}{}, WithID("round26-stop-1"))
	if err != nil {
		t.Fatal(err)
	}
	tickDone := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("renewal attempt did not start")
	}
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tickWorkflows did not return while ExtendLease stayed blocked (stopRenewal hung behind the call)")
	}
	// Turn ends ~1200ms; the abandoned loop exits with it (done closed),
	// so the tick returns around then — far below the stop's lease-scale
	// give-up (~2s cap after the ~1200ms stop) and infinitely below the
	// pre-fix unbounded wait.
	if elapsed := time.Since(start); elapsed >= 2500*time.Millisecond {
		t.Fatalf("tick took %v, want well before 2.5s (stop must join the exited loop, not wait out the blocked call)", elapsed)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "completed" {
		t.Fatalf("status=%s, want completed (turn covered through completion must commit)", info.Status)
	}
}

// round26LossyActorBackend fails ExtendLease with ErrNotFound for one
// instance (simulating the fenced-backend rejection a restarted worker's
// stale renewal gets after a peer reclaim advanced the generation) while
// delegating every other task's renewal to the wrapped backend.
type round26LossyActorBackend struct {
	backend.Backend
	lostInstance string
}

func (b *round26LossyActorBackend) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	if t.InstanceID == b.lostInstance {
		return backend.ErrNotFound
	}
	return b.Backend.ExtendLease(ctx, t, d)
}

// TestWorker_Round26_ActorAcquisitionCanceledOnLeaseLoss is the regression
// test for round-26 P2 (cancel actor acquisition when the turn loses its
// lease). A restarted worker reclaims a task whose instance actor is still
// held by an old context-ignoring turn; the new turn queues on the actor
// while its renewal already reports the lease lost. The dispatch wait must
// observe the canceled turn context — not just the live tick context — so
// the queued turn abandons acquisition (releasing its claim for a prompt
// peer retry) instead of blocking forever, holding its workflow slot and
// stalling wg.Wait past the flush of other completed turns.
//
// Layout: the test holds instance X's actor directly (standing in for the
// old ignoring turn), task X's renewal fails fast with ErrNotFound, task Y
// (another instance) runs a fast committing workflow in the same tick. The
// tick must return promptly with Y committed, X's turn never executed, and
// X's lease released. Without the fix the tick blocks on the held actor
// until cleanup releases it, failing the timeout.
func TestWorker_Round26_ActorAcquisitionCanceledOnLeaseLoss(t *testing.T) {
	ctx := context.Background()
	const lease = 500 * time.Millisecond
	mem := memory.New()
	store := &round26LossyActorBackend{Backend: mem, lostInstance: "round26-actor-x"}
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       lease,
		WorkerID:            "w1",
		ClaimLimit:          2,
		WorkflowConcurrency: 2,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	var blockEntered, fastEntered atomic.Int32
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		blockEntered.Add(1)
		return "blocked", nil
	}, WithName("block26WF"))
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		fastEntered.Add(1)
		return "done", nil
	}, WithName("fast26WF"))
	c := NewClient(mem)
	hx, err := Start(ctx, c, "block26WF", struct{}{}, WithID("round26-actor-x"))
	if err != nil {
		t.Fatal(err)
	}
	hy, err := Start(ctx, c, "fast26WF", struct{}{}, WithID("round26-actor-y"))
	if err != nil {
		t.Fatal(err)
	}
	// Hold X's actor for the whole tick: the old turn that ignores
	// context while the reclaimed turn queues behind it.
	actor := w.actorFor("round26-actor-x")
	actor.mu.Lock()
	defer actor.mu.Unlock()

	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tickWorkflows did not return (queued turn ignored its lease loss and blocked on the live tick ctx)")
	}
	if n := blockEntered.Load(); n != 0 {
		t.Fatalf("blocked workflow executed %d times, want 0 (lease-lost turn must abandon acquisition, never run)", n)
	}
	if n := fastEntered.Load(); n != 1 {
		t.Fatalf("fast workflow executed %d times, want 1 (sibling turn must flush while the loser abandons)", n)
	}
	info, err := c.Get(ctx, hy.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "completed" {
		t.Fatalf("sibling status=%s, want completed (wg.Wait must flush other turns)", info.Status)
	}
	infoX, err := c.Get(ctx, hx.ID())
	if err != nil {
		t.Fatal(err)
	}
	if infoX.Status != "running" {
		t.Fatalf("abandoned status=%s, want running (lost turn must be released, never committed)", infoX.Status)
	}
	// The abandoned claim was released for a prompt retry: a peer reclaims
	// it immediately instead of waiting out the lease.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: lease, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 || peer[0].InstanceID != "round26-actor-x" {
		t.Fatalf("peer reclaimed %d tasks, want exactly X's released turn", len(peer))
	}
}
