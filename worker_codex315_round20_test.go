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

// cancelDuringRequeueBackend fails workflow-state loads fast with a
// synthetic non-contention error (so the turn fails while the tick is
// still live) and pins the requeue NackTask on a test-controlled gate.
// ReleaseLease/NackTask are context-aware like a real store: calls with
// a canceled context are rejected without touching the lease, while
// detached (live) releases succeed.
type cancelDuringRequeueBackend struct {
	backend.Backend
	synthetic   error
	nackEntered chan struct{}
	nackRelease chan struct{}
	nackOnce    atomic.Bool
	releases    atomic.Int32
	nacks       atomic.Int32
}

func (b *cancelDuringRequeueBackend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	return nil, b.synthetic
}

func (b *cancelDuringRequeueBackend) NackTask(ctx context.Context, t backend.Task, d time.Duration) error {
	b.nacks.Add(1)
	if b.nackOnce.CompareAndSwap(false, true) {
		close(b.nackEntered)
	}
	<-b.nackRelease
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.NackTask(ctx, t, d)
}

func (b *cancelDuringRequeueBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_Round20_FailedTurnTrackedUntilRequeueCompletes is the
// regression test for round-20 P2 (keep failed turns tracked until
// requeue completes). Shutdown canceling the tick after the live-tick
// check but before/during requeueWorkflowTask used to hit an already
// untracked task: the requeue's release/nack on the canceled ctx is
// rejected by a context-aware backend, and releaseInFlight cannot find
// the untracked task either — so the renewed lease stays hidden until
// expiry.
//
// With the fix the turn stays tracked through the requeue, and a tick
// canceled mid-requeue falls back to the detached abandonment release
// (live detached ctx, ownership-gated): the peer retries promptly.
// Without the fix no release is ever issued and the peer claims nothing
// until expiry.
func TestWorker_Round20_FailedTurnTrackedUntilRequeueCompletes(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &cancelDuringRequeueBackend{
		Backend:     mem,
		synthetic:   errors.New("round20 synthetic load failure"),
		nackEntered: make(chan struct{}),
		nackRelease: make(chan struct{}),
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
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID("round20-requeue-1")); err != nil {
		t.Fatal(err)
	}

	tickCtx, cancel := context.WithCancel(context.Background())
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(tickCtx)
	}()
	// The turn fails fast (synthetic load error, tick still live) and
	// blocks inside the requeue NackTask.
	select {
	case <-store.nackEntered:
	case <-time.After(10 * time.Second):
		cancel()
		close(store.nackRelease)
		t.Fatal("failed turn never reached requeue NackTask")
	}
	// Shutdown cancels mid-requeue; then let the pinned nack observe the
	// canceled tick ctx. Old code untracked before the requeue, so the
	// rejected nack leaves the task orphaned.
	cancel()
	close(store.nackRelease)
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return (requeue + detached release must not hang on cancellation)")
	}

	// The canceled-mid-requeue turn must have abandoned via the detached
	// release, not vanished untracked: exactly one release, and the peer
	// retries without waiting for expiry.
	if n := store.nacks.Load(); n != 1 {
		t.Fatalf("NackTask calls = %d, want 1 (failed turn must attempt its requeue before falling back)", n)
	}
	if n := store.releases.Load(); n != 1 {
		t.Fatalf("ReleaseLease calls = %d, want 1 (tick canceled mid-requeue must abandon via one detached release, not leave the lease hidden until expiry)", n)
	}
	peers, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: 30 * time.Second, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].InstanceID != "round20-requeue-1" {
		got := make([]string, 0, len(peers))
		for _, p := range peers {
			got = append(got, p.InstanceID)
		}
		t.Fatalf("peer claimed %v, want exactly [round20-requeue-1] (detached-released lease must be promptly reclaimable, not stuck until expiry)", got)
	}
	w.mu.Lock()
	left := len(w.inFlight)
	w.mu.Unlock()
	if left != 0 {
		t.Fatalf("inFlight = %d after the tick, want 0 (abandoned turn must leave nothing tracked)", left)
	}
}
