package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// snapshotClaimBackend models a context-ignoring store delivering a
// pre-restart claim snapshot post-restart: the first activity ClaimTasks
// evaluates against the store immediately, then blocks on a test gate
// (ignoring context cancellation) before returning the frozen rows. Later
// claims delegate directly.
type snapshotClaimBackend struct {
	backend.Backend
	gate    chan struct{}
	entered chan struct{}
	armed   atomic.Bool
}

func (b *snapshotClaimBackend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	if req.Kind != "activity" || !b.armed.CompareAndSwap(false, true) {
		return b.Backend.ClaimTasks(ctx, req)
	}
	snap, err := b.Backend.ClaimTasks(context.Background(), req)
	close(b.entered)
	<-b.gate
	return snap, err
}

// TestWorker_SupersededGenerationActivityClaimsDropped is the
// regression test for round-21 P1 (reject claims from a superseded worker
// generation). An activity ClaimTasks blocked across a Shutdown timeout +
// restart returns a stale claim post-restart; the old code stamped the
// CURRENT epoch onto it via trackTaskAt — overwriting an entry the
// restarted loop already re-tracked for the same task ID — and dispatched
// it, so the stale goroutine's token-fenced untrack removed the live entry
// and the genuine new invocation failed its token checks, discarding its
// result after side effects.
//
// With the fix the tick binds its claims to the issuing generation:
// the batch epoch check drops every stale claim via a detached fenced
// release and dispatches nothing. Without the fix the stale activity runs
// (ran != 0) and the task stays tracked instead of promptly reclaimable.
func TestWorker_SupersededGenerationActivityClaimsDropped(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round21-p1-1", "hooked")
	// Expire the setup probe's short claim lease (memory runs on a manual
	// clock) so the task is claimable below.
	mem.SetNow(time.Now().UTC().Add(2 * time.Second))

	store := &snapshotClaimBackend{
		Backend: mem,
		gate:    make(chan struct{}),
		entered: make(chan struct{}),
	}
	var ran atomic.Int32
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          30 * time.Second,
		WorkerID:               "w1",
		ActivityConcurrency:    4,
		IncompatibleRetryDelay: -1,
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		ran.Add(1)
		return "ok", nil
	}, WithName("hooked"))

	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickActivities(ctx)
	}()
	select {
	case <-store.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("tickActivities never reached ClaimTasks")
	}
	// Simulate a Shutdown timeout + restart while the claim is blocked:
	// the generation moves on (StartWithError increments epoch).
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	close(store.gate)
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tickActivities did not return after the stale claim was delivered")
	}
	// The stale dispatch is asynchronous; give a canceled generation a
	// moment to (incorrectly) run before asserting it never did.
	deadline := time.Now().Add(500 * time.Millisecond)
	for ran.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := ran.Load(); n != 0 {
		t.Fatalf("stale activity ran %d times, want 0 (superseded generation must not dispatch)", n)
	}
	w.mu.Lock()
	nFlight := len(w.inFlight)
	w.mu.Unlock()
	if nFlight != 0 {
		t.Fatalf("inFlight has %d entries, want 0 (stale claim must not be tracked)", nFlight)
	}
	// The detached fenced release made the task promptly reclaimable.
	retry, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 {
		t.Fatalf("peer reclaimed %d tasks, want 1 (stale claim must be fenced-released, not linger until expiry)", len(retry))
	}
}

// TestWorker_StaleTrackRejectedWithoutTouchingNewEntry pins the
// atomic check-and-stamp in trackTaskAtEpoch: a stale generation's track
// must be rejected and must leave an entry re-tracked by the restarted loop
// for the same task ID untouched. (This test names the new helper, so it
// fails to compile without the fix; the behavioral fail-without-fix is
// covered by TestWorker_SupersededGenerationActivityClaimsDropped.)
func TestWorker_StaleTrackRejectedWithoutTouchingNewEntry(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{LeaseDuration: 30 * time.Second, WorkerID: "w1"})
	fake := backend.Task{ID: 4242, Kind: "activity", InstanceID: "i", WorkerID: "w1", Attempt: 2}

	staleEpoch := w.claimEpoch()
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	liveEpoch := w.claimEpoch()

	tokNew, ok := w.trackTaskAtEpoch(fake, time.Now(), liveEpoch)
	if !ok {
		t.Fatal("live generation track rejected, want accept")
	}
	if _, ok := w.trackTaskAtEpoch(fake, time.Now(), staleEpoch); ok {
		t.Fatal("stale generation track accepted, want reject")
	}
	w.mu.Lock()
	e, present := w.inFlight[fake.ID]
	w.mu.Unlock()
	if !present {
		t.Fatal("live entry missing after stale track attempt (must be left intact)")
	}
	if e.epoch != tokNew.epoch || e.seq != tokNew.seq {
		t.Fatal("live entry overwritten by stale track (must be left intact)")
	}
}

// TestWorker_StaleWorkflowFlushSkipped is the regression test for
// round-21 P2 (keep workflow flushes fenced after shutdown releases the
// claim). A Shutdown timeout fires after a workflow turn finished
// (pending) but before the tick's flush: releaseInFlight releases the
// still-tracked pending to a peer, the peer reclaims it, and the old tick's
// flush continues on its detached commit ctx. The old flush validated task
// ID + sequence only, so it committed and deleted the peer-claimed task.
//
// With the fix the flush is ownership-gated (skipStaleCommit): entries that
// lost in-flight ownership are skipped without touching the store. Without
// the fix the stale advancement is committed (journal advances, peer task
// deleted). The test drives only production paths, so it compiles — and
// fails — without the fix.
func TestWorker_StaleWorkflowFlushSkipped(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration:       30 * time.Second,
		WorkerID:            "w1",
		WorkflowConcurrency: 1,
		ClaimLimit:          1,
	})
	entered := make(chan struct{})
	releaseTurn := make(chan struct{})
	var turnRan atomic.Int32
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		turnRan.Add(1)
		close(entered)
		// Ignore cancellation like a ctx-insensitive turn: the shutdown
		// below must win by ownership, not by cooperation.
		<-releaseTurn
		return "done", nil
	}, WithName("WF"))
	c := client.NewClient(mem)
	if _, err := client.Start(ctx, c, "WF", struct{}{}, client.WithID("round21-p2-1")); err != nil {
		t.Fatal(err)
	}

	tickCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("workflow turn never started")
	}
	// Shutdown timeout: cancel the loop, then release leases while the old
	// tick is still inside handleWorkflow (renewal join omitted: workflow
	// turns hold no renewal in this branch).
	cancel()
	relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
	w.releaseInFlight(relCtx)
	relCancel()
	// Simulate the restart's generation move so a re-tracked entry could
	// never match the stale token even if IDs collide.
	w.mu.Lock()
	w.epoch++
	w.mu.Unlock()
	// A peer reclaims the released turn.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatal("shutdown release did not hand the turn to a peer (want 1 reclaimable task)")
	}
	// The old tick finishes on its detached flush ctx.
	close(releaseTurn)
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return after the turn was released")
	}
	if n := turnRan.Load(); n != 1 {
		t.Fatalf("turn ran %d times, want 1", n)
	}
	// The stale flush must not have committed: no completion event.
	st, err := mem.LoadWorkflow(ctx, "round21-p2-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range st.Journal {
		if e.Type == journal.TypeWorkflowCompleted {
			t.Fatal("stale flush committed a peer-owned task (journal advanced after shutdown release)")
		}
	}
	// And the peer lease must be intact (still hidden, not deleted).
	again, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "peer2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("peer lease disturbed: %d tasks claimable, want 0", len(again))
	}
}
