package tasuki

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorker_Round21_LeaseLossCancelsWorkflowTurn is the regression test
// for round-21 P1 (cancel the workflow turn after lease ownership is
// lost). A peer reclaims the turn mid-execution (store clock advanced past
// the lease, peer claims with a new attempt); the stale renewal then gets a
// REAL ErrNotFound from the fenced backend. The old code only exited the
// renewal goroutine while handleWorkflow and its local activities
// continued on the live tick ctx — the stale worker performed local side
// effects concurrently with the peer's turn (its fenced commit was
// eventually rejected, but the side effects were already duplicated).
//
// With the fix the loss cancels the per-turn context: the in-flight local
// activity observes the cancellation promptly, later ExecuteLocal calls are
// rejected pre-invoke, and the turn is abandoned (released, never
// committed). Without the fix the second local activity runs to completion
// and the tick only ends after the backstop delay.
func TestWorker_Round21_LeaseLossCancelsWorkflowTurn(t *testing.T) {
	ctx := context.Background()
	const lease = time.Second
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	w := NewWorker(mem, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       lease,
		WorkerID:            "w1",
		ClaimLimit:          1,
		WorkflowConcurrency: 1,
	})
	var firstDone, secondDone atomic.Int32
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		firstDone.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "first", nil
	}, WithName("firstLocal"))
	RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-actCtx.Done():
			return "", actCtx.Err()
		case <-time.After(5 * time.Second):
			// Backstop so the pre-fix run terminates: the side effect
			// below is exactly the duplication the fix prevents.
			secondDone.Add(1)
			return "second", nil
		}
	}, WithName("secondLocal"))
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		if _, err := workflow.ExecuteLocal[struct{}, string](wctx, "firstLocal", struct{}{}); err != nil {
			return "", err
		}
		if _, err := workflow.ExecuteLocal[struct{}, string](wctx, "secondLocal", struct{}{}); err != nil {
			return "", err
		}
		return "done", nil
	}, WithName("chainWF"))
	c := NewClient(mem)
	h, err := Start(ctx, c, "chainWF", struct{}{}, WithID("round21-loss-1"))
	if err != nil {
		t.Fatal(err)
	}

	tickDone := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	// Wait until the turn is inside the second local activity, then let a
	// peer reclaim it: advance the store clock past the lease and claim
	// with a new attempt, well before the next renewal tick (lease/2).
	deadline := time.Now().Add(10 * time.Second)
	for firstDone.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if firstDone.Load() == 0 {
		t.Fatal("turn never reached the first local activity")
	}
	mem.SetNow(t0.Add(lease + 100*time.Millisecond))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: lease, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatal("peer did not reclaim the expired turn (want 1 task)")
	}
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return after lease loss")
	}
	elapsed := time.Since(start)
	if n := secondDone.Load(); n != 0 {
		t.Fatalf("second local side effect ran %d times, want 0 (stale turn must abandon on lease loss, not execute beside the peer)", n)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("tick took %v, want <2s (lease loss must cancel the turn promptly, not run it to the backstop)", elapsed)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "running" {
		t.Fatalf("status=%s, want running (lost turn must be abandoned, never committed)", info.Status)
	}
	// The peer lease is intact: the abandoned turn released nothing twice
	// and committed nothing over it.
	again, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: lease, WorkerID: "peer2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("peer lease disturbed: %d tasks claimable, want 0", len(again))
	}
}

// flakyExtendBackend fails the first N ExtendLease calls with a transient
// error, then delegates. It optionally tracks the store clock with wall
// time so lease stamps and expiries behave like a production backend
// (memory runs on a manual clock): every renewal attempt first advances
// the store clock to test-start + wall-elapsed.
type flakyExtendBackend struct {
	backend.Backend
	mem       *memory.Backend
	t0        time.Time
	wallStart time.Time
	mu        sync.Mutex
	calls     int
	failFirst int
	successAt time.Time
}

func (b *flakyExtendBackend) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	b.mu.Lock()
	b.calls++
	n := b.calls
	b.mu.Unlock()
	if b.mem != nil {
		b.mem.SetNow(b.t0.Add(time.Since(b.wallStart)))
	}
	if n <= b.failFirst {
		return errors.New("round21 transient extend failure")
	}
	err := b.Backend.ExtendLease(ctx, t, d)
	if err == nil {
		b.mu.Lock()
		if b.successAt.IsZero() {
			b.successAt = time.Now()
		}
		b.mu.Unlock()
	}
	return err
}

func (b *flakyExtendBackend) extendCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// TestWorker_Round21_RenewalRetryRecoversTransientFailure is the regression
// test for round-21 P2 (retry failed renewals before the current lease
// expires), transient half. The first two renewals fail transiently; with
// the fix a prompt in-window retry succeeds (~1.3s into a 2s lease), so the
// original deadline passes with the lease held: a peer claiming then gets
// nothing and the turn commits. Without the fix the worker idles until the
// next half-lease tick — which lands at the original deadline — so the peer
// reclaims first (concurrent execution after one transient blip) and the
// fenced commit is rejected.
func TestWorker_Round21_RenewalRetryRecoversTransientFailure(t *testing.T) {
	ctx := context.Background()
	const lease = 2 * time.Second
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &flakyExtendBackend{Backend: mem, mem: mem, t0: t0, failFirst: 2}
	store.wallStart = time.Now()
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       lease,
		WorkerID:            "w1",
		ClaimLimit:          1,
		WorkflowConcurrency: 1,
	})
	// A turn longer than the lease: renewal must cover it mid-execution.
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		time.Sleep(4 * time.Second)
		return "done", nil
	}, WithName("slowWF"))
	c := NewClient(store)
	h, err := Start(ctx, c, "slowWF", struct{}{}, WithID("round21-retry-1"))
	if err != nil {
		t.Fatal(err)
	}

	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	// Past the original deadline (claim + lease) but before the turn ends:
	// the store clock is advanced to match, like production.
	time.Sleep(2500 * time.Millisecond)
	mem.SetNow(t0.Add(2500 * time.Millisecond))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: lease, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 0 {
		t.Fatalf("peer reclaimed %d tasks at the original deadline, want 0 (in-window retry must hold the lease across a transient blip)", len(peer))
	}
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return")
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "completed" {
		t.Fatalf("status=%s, want completed (turn covered by the retry must commit)", info.Status)
	}
}

// deadExtendBackend fails every ExtendLease with a transient error: the
// lease is unrestorable, so the turn must be abandoned before expiry.
type deadExtendBackend struct {
	backend.Backend
	calls atomic.Int32
}

func (b *deadExtendBackend) ExtendLease(context.Context, backend.Task, time.Duration) error {
	b.calls.Add(1)
	return errors.New("round21 persistent extend failure")
}

// TestWorker_Round21_PersistentRenewalFailureAbandonsBeforeExpiry is the
// regression test for round-21 P2 (retry failed renewals before the
// current lease expires), persistent half. Every renewal fails; with the
// fix bounded backoff retries inside the remaining window give up near
// expiry and cancel the turn via the lease-loss path — the in-flight local
// activity observes the cancellation, the lease is released promptly, and
// nothing commits. Without the fix the worker idles between half-lease
// ticks with no cancellation: the turn runs to its (long) activity end and
// only then discovers the lease is gone.
func TestWorker_Round21_PersistentRenewalFailureAbandonsBeforeExpiry(t *testing.T) {
	ctx := context.Background()
	const lease = 2 * time.Second
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &deadExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:        time.Hour, // tick driven manually below
		LeaseDuration:       lease,
		WorkerID:            "w1",
		ClaimLimit:          1,
		WorkflowConcurrency: 1,
	})
	var sawCancel atomic.Int32
	RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		select {
		case <-actCtx.Done():
			sawCancel.Add(1)
			return "", actCtx.Err()
		case <-time.After(6 * time.Second):
			return "slow", nil
		}
	}, WithName("slowLocal"))
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		out, err := workflow.ExecuteLocal[struct{}, string](wctx, "slowLocal", struct{}{})
		if err != nil {
			return "", err
		}
		return out, nil
	}, WithName("doomedWF"))
	c := NewClient(store)
	h, err := Start(ctx, c, "doomedWF", struct{}{}, WithID("round21-doomed-1"))
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		w.tickWorkflows(ctx)
	}()
	select {
	case <-tickDone:
	case <-time.After(15 * time.Second):
		t.Fatal("tickWorkflows did not return after unrestorable renewal")
	}
	elapsed := time.Since(start)
	// Abandon lands at lease - margin (500ms margin on a 2s lease ≈ 1.5s,
	// plus backoff granularity); it must precede the 2s original deadline
	// a peer would reclaim at, with slack for CI scheduling.
	if elapsed >= 2400*time.Millisecond {
		t.Fatalf("tick took %v, want <2.4s (unrestorable lease must abandon the turn pre-expiry, not run to the 6s activity end)", elapsed)
	}
	if n := sawCancel.Load(); n != 1 {
		t.Fatalf("local activity observed %d cancellations, want 1 (abandon must propagate through the turn context)", n)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "running" {
		t.Fatalf("status=%s, want running (abandoned turn must never commit)", info.Status)
	}
	// Prompt release, not expiry wait: the abandoned turn is reclaimable now.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: lease, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer reclaimed %d tasks, want 1 (abandon must release promptly)", len(peer))
	}
}
