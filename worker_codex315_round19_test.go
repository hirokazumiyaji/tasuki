package tasuki

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorker_Round19_LocalRunnerRejectsCanceledRunCtx is the regression
// test for round-19 P2a (check cancellation before invoking another local
// activity). A workflow that catches cancellation from one local activity
// and calls ExecuteLocal again passes the same already-canceled runCtx;
// both branches still invoked act.fn before checking (the timeout-enabled
// branch launched the goroutine before checking the canceled parent), so
// side effects ran post-shutdown. The guard was post-call only.
//
// With the fix an already-canceled runCtx is rejected BEFORE invoking in
// both branches: no fn call and no goroutine launch. Without the fix the
// second activity's fn runs (calls==1) even though the call returns the
// cancellation.
func TestWorker_Round19_LocalRunnerRejectsCanceledRunCtx(t *testing.T) {
	for _, timeout := range []time.Duration{0, 30 * time.Second} {
		mem := memory.New()
		mem.SetNow(time.Now().UTC())
		w := NewWorker(mem, WorkerOptions{
			LeaseDuration:        30 * time.Second,
			WorkerID:             "w1",
			LocalActivityTimeout: timeout,
		})
		var calls atomic.Int32
		RegisterActivity(w, func(context.Context, struct{}) (string, error) {
			calls.Add(1)
			return "second", nil
		}, WithName("second"))

		runCtx, cancel := context.WithCancel(context.Background())
		cancel() // already canceled before the second ExecuteLocal

		wctx := workflow.NewContext(nil, time.Now().UTC())
		w.attachLocalActivityRunner(wctx, runCtx)
		_, err := workflow.ExecuteLocal[struct{}, string](wctx, "second", struct{}{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("timeout=%v: ExecuteLocal err = %v, want context.Canceled (already-canceled runCtx must be rejected)", timeout, err)
		}
		// The timeout-enabled branch invokes fn on a goroutine: give a
		// launched goroutine time to run so a missing pre-call check
		// cannot hide behind scheduling delay.
		deadline := time.Now().Add(500 * time.Millisecond)
		for calls.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("timeout=%v: second activity invoked %d times, want 0 (canceled runCtx must return before invoking fn or launching a goroutine)", timeout, n)
		}

		// Sanity: a live runCtx still invokes normally in both branches.
		liveCtx := context.Background()
		wctx2 := workflow.NewContext(nil, time.Now().UTC())
		w.attachLocalActivityRunner(wctx2, liveCtx)
		got, err := workflow.ExecuteLocal[struct{}, string](wctx2, "second", struct{}{})
		if err != nil {
			t.Fatalf("timeout=%v: live ExecuteLocal err = %v, want nil", timeout, err)
		}
		if got != "second" {
			t.Fatalf("timeout=%v: live ExecuteLocal = %q, want %q", timeout, got, "second")
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("timeout=%v: live calls = %d, want 1", timeout, n)
		}
	}
}

// raceErrBackend reproduces Shutdown racing an early non-cancel workflow
// error: LoadWorkflowHead blocks until the test cancels the tick, then
// returns a synthetic non-cancel error. ReleaseLease/NackTask are
// context-aware like a real store: calls with a canceled context are
// rejected without touching the lease, while detached (live) releases
// succeed.
type raceErrBackend struct {
	backend.Backend
	loadEntered chan struct{}
	loadRelease chan struct{}
	enterOnce   atomic.Bool
	synthetic   error
	releases    atomic.Int32
	nacks       atomic.Int32
}

func (b *raceErrBackend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.loadEntered)
	}
	<-b.loadRelease
	return nil, b.synthetic
}

func (b *raceErrBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.ReleaseLease(ctx, t)
}

func (b *raceErrBackend) NackTask(ctx context.Context, t backend.Task, d time.Duration) error {
	b.nacks.Add(1)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.NackTask(ctx, t, d)
}

// TestWorker_Round19_CanceledTickAbandonsEarlyNonCancelError is the
// regression test for round-19 P2b (abandon generic handler errors once
// the tick is canceled). Shutdown racing an early non-cancel error
// (loadWorkflowState/registry lookup) missed the abandon path — herr is
// not a context error — and fell through to untrack + nack/release with
// the canceled tick ctx, which a context-aware backend rejects. The task
// ends up untracked so neither this path nor Shutdown's releaseInFlight
// can release it, and failover waits for lease expiry.
//
// With the fix ANY canceled tick abandons regardless of herr, via an
// ownership-gated detached release: the peer can claim the task
// promptly. Without the fix the peer claims nothing until expiry.
func TestWorker_Round19_CanceledTickAbandonsEarlyNonCancelError(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &raceErrBackend{
		Backend:     mem,
		loadEntered: make(chan struct{}),
		loadRelease: make(chan struct{}),
		synthetic:   errors.New("round19 synthetic load failure"),
	}
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       30 * time.Second,
		WorkerID:            "w1",
		ClaimLimit:          10,
		WorkflowConcurrency: 4,
	})
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(store)
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID("round19-early-err-1")); err != nil {
		t.Fatal(err)
	}

	tickCtx, cancel := context.WithCancel(context.Background())
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()
	// The turn is now blocked inside LoadWorkflowHead with the tick
	// still live (its entry guard passed legitimately).
	select {
	case <-store.loadEntered:
	case <-time.After(10 * time.Second):
		cancel()
		close(store.loadRelease)
		t.Fatal("workflow turn never reached LoadWorkflowHead")
	}
	// Shutdown cancels mid-turn; then let the blocked load fail with a
	// NON-cancel error. Old code sees a canceled tick with a generic
	// herr and takes the untrack+nack path with the dead ctx.
	cancel()
	close(store.loadRelease)
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return (abandon must not hang on cancellation)")
	}

	// The canceled tick must have abandoned via the detached release,
	// not the canceled-ctx nack: the lease is released promptly and the
	// peer retries without waiting for expiry.
	if n := store.releases.Load(); n != 1 {
		t.Fatalf("ReleaseLease calls = %d, want 1 (canceled tick must abandon via one detached release, not a rejected canceled-ctx nack)", n)
	}
	peers, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: 30 * time.Second, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].InstanceID != "round19-early-err-1" {
		got := make([]string, 0, len(peers))
		for _, p := range peers {
			got = append(got, p.InstanceID)
		}
		t.Fatalf("peer claimed %v, want exactly [round19-early-err-1] (abandoned lease must be promptly reclaimable, not stuck until expiry)", got)
	}
	w.mu.Lock()
	left := len(w.inFlight)
	w.mu.Unlock()
	if left != 0 {
		t.Fatalf("inFlight = %d after the tick, want 0 (abandoned turn must leave nothing tracked)", left)
	}
}
