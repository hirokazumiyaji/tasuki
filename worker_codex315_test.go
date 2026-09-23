package tasuki_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// TestWorkflow_ShutdownAbandonsTurnNotFailed is a regression test for the
// shutdown-cancellation finding: when Shutdown cancels a cooperative local
// activity, the turn must be abandoned (lease released for a peer) instead of
// committed as a terminal workflow failure.
func TestWorkflow_ShutdownAbandonsTurnNotFailed(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	started := make(chan struct{}, 1)
	var calls atomic.Int32
	blockLocal := func(actCtx context.Context, _ struct{}) (string, error) {
		if calls.Add(1) == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-actCtx.Done():
				return "", actCtx.Err()
			case <-time.After(10 * time.Second):
				return "slow-first", nil
			}
		}
		return "recovered", nil
	}
	blockWF := func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "blockLocal", struct{}{})
	}

	w1 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w1, blockLocal, tasuki.WithName("blockLocal"))
	tasuki.RegisterWorkflow(w1, blockWF, tasuki.WithName("abandonWF"))
	w1.Start(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "abandonWF", struct{}{}, tasuki.WithID("shutdown-abandon-1"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local activity did not start")
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w1.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("calls=%d, want 1 (no duplicate execution during shutdown)", n)
	}

	// The canceled turn must not be persisted as a failure: the instance
	// stays running and its lease is released for a peer.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info == nil {
		t.Fatal("missing instance info after shutdown")
	}
	if info.Status != "running" {
		t.Fatalf("status=%s failure=%s, want running (shutdown must not fail the workflow)",
			info.Status, string(info.Failure))
	}

	// A peer worker must be able to retry and complete the workflow.
	w2 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w2",
	})
	tasuki.RegisterActivity(w2, blockLocal, tasuki.WithName("blockLocal"))
	tasuki.RegisterWorkflow(w2, blockWF, tasuki.WithName("abandonWF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)

	resCtx, resCancel := context.WithTimeout(ctx, 10*time.Second)
	defer resCancel()
	got, err := tasuki.Result[string](resCtx, h)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != "recovered" {
		t.Fatalf("got %q want %q", got, "recovered")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("calls=%d, want 2 (canceled attempt + peer retry)", n)
	}
}

// TestWorkflow_LeaseRenewalCoversBatchCommit is a regression test for the
// renewal-lifetime finding: with WorkflowConcurrency > 1, a finished task's
// lease must keep renewing until the batch commit deletes it. Otherwise a
// slow sibling lets the finished lease expire mid-batch and a peer
// duplicates the execution.
func TestWorkflow_LeaseRenewalCoversBatchCommit(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	// Advance virtual time at wall speed so leases can expire mid-batch.
	stopAdv := make(chan struct{})
	defer close(stopAdv)
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopAdv:
				return
			case <-tk.C:
				b.SetNow(b.Now().Add(10 * time.Millisecond))
			}
		}
	}()

	var fastCalls, slowCalls atomic.Int32
	fastLocal := func(context.Context, int) (int, error) {
		fastCalls.Add(1)
		return 42, nil
	}
	slowLocal := func(actCtx context.Context, n int) (int, error) {
		slowCalls.Add(1)
		select {
		case <-actCtx.Done():
			return 0, actCtx.Err()
		case <-time.After(600 * time.Millisecond):
			return n * 2, nil
		}
	}
	fastWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "fastLocal", n)
	}
	slowWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "slowLocal", n)
	}
	mkWorker := func(id string) *tasuki.Worker {
		w := tasuki.NewWorker(b, tasuki.WorkerOptions{
			PollInterval:        5 * time.Millisecond,
			LeaseDuration:       150 * time.Millisecond,
			WorkflowConcurrency: 2,
			ClaimLimit:          2,
			WorkerID:            id,
		})
		tasuki.RegisterActivity(w, fastLocal, tasuki.WithName("fastLocal"))
		tasuki.RegisterActivity(w, slowLocal, tasuki.WithName("slowLocal"))
		tasuki.RegisterWorkflow(w, fastWF, tasuki.WithName("batchFastWF"))
		tasuki.RegisterWorkflow(w, slowWF, tasuki.WithName("batchSlowWF"))
		return w
	}
	w1 := mkWorker("w1")
	w1.Start(ctx)
	defer w1.Shutdown(ctx)

	c := tasuki.NewClient(b)
	fh, err := tasuki.Start(ctx, c, "batchFastWF", 21, tasuki.WithID("batch-commit-fast"))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := tasuki.Start(ctx, c, "batchSlowWF", 7, tasuki.WithID("batch-commit-slow"))
	if err != nil {
		t.Fatal(err)
	}
	// Let w1 claim both tasks into one batch before the peer arrives; the
	// slow turn (~600ms) then spans several lease periods of the fast task.
	time.Sleep(250 * time.Millisecond)
	w2 := mkWorker("w2")
	w2.Start(ctx)
	defer w2.Shutdown(ctx)

	waitResult := func(h *tasuki.Handle, want int, name string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			info, err := c.Get(ctx, h.ID())
			if err != nil {
				t.Fatal(err)
			}
			if info != nil && info.Status == "completed" {
				got, err := tasuki.Result[int](ctx, h)
				if err != nil {
					t.Fatalf("%s Result: %v", name, err)
				}
				if got != want {
					t.Fatalf("%s got %d want %d", name, got, want)
				}
				return
			}
			if info != nil && (info.Status == "failed" || info.Status == "stuck") {
				t.Fatalf("%s status=%s failure=%s", name, info.Status, string(info.Failure))
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s timeout waiting for completion", name)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitResult(fh, 42, "fast")
	waitResult(sh, 14, "slow")
	if n := fastCalls.Load(); n != 1 {
		t.Fatalf("fast local activity executed %d times, want exactly once (lease must cover batch commit)", n)
	}
	if n := slowCalls.Load(); n != 1 {
		t.Fatalf("slow local activity executed %d times, want exactly once", n)
	}
}

// TestWorkflow_LocalActivityTimeoutNonCooperative verifies
// WorkerOptions.LocalActivityTimeout is enforced outside the activity call:
// an activity that ignores cancellation still yields a deadline error on
// time instead of blocking the turn indefinitely.
func TestWorkflow_LocalActivityTimeoutNonCooperative(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         5 * time.Millisecond,
		LeaseDuration:        time.Minute,
		LocalActivityTimeout: 50 * time.Millisecond,
	})
	tasuki.RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		time.Sleep(5 * time.Second) // ignores cancellation entirely
		return "slow", nil
	}, tasuki.WithName("stubbornLocal"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "stubbornLocal", struct{}{})
	}, tasuki.WithName("stubbornWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "stubbornWF", struct{}{}, tasuki.WithID("local-timeout-stubborn-1"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = tasuki.Result[string](resCtx, h)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout failure")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
	// The activity sleeps 5s; without external enforcement the turn (and
	// this Result) would block for the full 5s.
	if elapsed > 3*time.Second {
		t.Fatalf("Result took %v; timeout was not enforced outside the call", elapsed)
	}
}

// TestWorkflow_ShutdownAbandonsTurnDespiteSwallowedCancel is a regression
// test for the follow-up finding on cc71aa48: the abandon guard must check
// the turn context before processing any engine result. Here the local
// activity observes Shutdown cancellation but returns nil (swallowing it),
// so the workflow completes normally — without the guard, the shutdown would
// be persisted as a completion instead of abandoned for a peer retry.
func TestWorkflow_ShutdownAbandonsTurnDespiteSwallowedCancel(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())

	started := make(chan struct{}, 1)
	var calls atomic.Int32
	swallowLocal := func(actCtx context.Context, _ struct{}) (string, error) {
		if calls.Add(1) == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-actCtx.Done()
			return "shutdown-value", nil
		}
		return "recovered", nil
	}
	swallowWF := func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ExecuteLocal[struct{}, string](wctx, "swallowLocal", struct{}{})
	}

	w1 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w1, swallowLocal, tasuki.WithName("swallowLocal"))
	tasuki.RegisterWorkflow(w1, swallowWF, tasuki.WithName("swallowWF"))
	w1.Start(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "swallowWF", struct{}{}, tasuki.WithID("shutdown-swallow-1"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local activity did not start")
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w1.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("calls=%d, want 1 (no duplicate execution during shutdown)", n)
	}

	// The swallowed cancellation must not be persisted as a completion.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info == nil {
		t.Fatal("missing instance info after shutdown")
	}
	if info.Status != "running" {
		t.Fatalf("status=%s, want running (shutdown must not complete the workflow)", info.Status)
	}

	// A peer worker must be able to retry and complete the workflow.
	w2 := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w2",
	})
	tasuki.RegisterActivity(w2, swallowLocal, tasuki.WithName("swallowLocal"))
	tasuki.RegisterWorkflow(w2, swallowWF, tasuki.WithName("swallowWF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)

	resCtx, resCancel := context.WithTimeout(ctx, 10*time.Second)
	defer resCancel()
	got, err := tasuki.Result[string](resCtx, h)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != "recovered" {
		t.Fatalf("got %q want %q", got, "recovered")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("calls=%d, want 2 (canceled attempt + peer retry)", n)
	}
}

// nackExtendSpy records ExtendLease calls for tasks that were already
// nacked, detecting renewal that outlives a nack and overwrites
// NackTask's visible_at (IncompatibleRetryDelay) with the lease duration.
type nackExtendSpy struct {
	backend.Backend
	mu               sync.Mutex
	nacked           map[int64]struct{}
	extendsAfterNack int
}

func (s *nackExtendSpy) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	err := s.Backend.NackTask(ctx, t, delay)
	if err == nil {
		s.mu.Lock()
		if s.nacked == nil {
			s.nacked = map[int64]struct{}{}
		}
		s.nacked[t.ID] = struct{}{}
		s.mu.Unlock()
	}
	return err
}

func (s *nackExtendSpy) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	s.mu.Lock()
	if _, ok := s.nacked[t.ID]; ok {
		s.extendsAfterNack++
	}
	s.mu.Unlock()
	return s.Backend.ExtendLease(ctx, t, d)
}

func (s *nackExtendSpy) countExtendsAfterNack() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.extendsAfterNack
}

// TestWorkflow_NackStopsLeaseRenewal is a regression test for the follow-up
// finding on cc71aa48: a nacked workflow task must stop lease renewal
// immediately instead of renewing until the batch flush. Otherwise a slow
// sibling keeps ExtendLease overwriting the nack's visible_at and delays a
// compatible worker during rolling deployment.
func TestWorkflow_NackStopsLeaseRenewal(t *testing.T) {
	ctx := context.Background()
	spy := &nackExtendSpy{Backend: memory.New()}
	spy.Backend.(*memory.Backend).SetNow(time.Now().UTC())

	slowLocal := func(actCtx context.Context, n int) (int, error) {
		select {
		case <-actCtx.Done():
			return 0, actCtx.Err()
		case <-time.After(600 * time.Millisecond):
			return n * 2, nil
		}
	}
	slowWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "slowLocal", n)
	}

	c := tasuki.NewClient(spy)
	// Start both instances before the worker so one tick claims them into a
	// single batch: the ghost nacks immediately while the slow turn spans
	// several lease periods (~600ms vs 150ms lease).
	if _, err := tasuki.Start(ctx, c, "ghostWF", struct{}{}, tasuki.WithID("nack-renewal-ghost")); err != nil {
		t.Fatal(err)
	}
	sh, err := tasuki.Start(ctx, c, "slowWF", 7, tasuki.WithID("nack-renewal-slow"))
	if err != nil {
		t.Fatal(err)
	}

	w := tasuki.NewWorker(spy, tasuki.WorkerOptions{
		PollInterval:        5 * time.Millisecond,
		LeaseDuration:       150 * time.Millisecond,
		WorkflowConcurrency: 2,
		ClaimLimit:          2,
		WorkerID:            "w1",
	})
	tasuki.RegisterActivity(w, slowLocal, tasuki.WithName("slowLocal"))
	tasuki.RegisterWorkflow(w, slowWF, tasuki.WithName("slowWF"))
	// NOTE: ghostWF is deliberately unregistered so its task is nacked.
	w.Start(ctx)
	defer w.Shutdown(ctx)

	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := c.Get(ctx, sh.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info != nil && info.Status == "completed" {
			break
		}
		if info != nil && (info.Status == "failed" || info.Status == "stuck") {
			t.Fatalf("slow status=%s failure=%s", info.Status, string(info.Failure))
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for slow workflow completion")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := spy.countExtendsAfterNack(); n != 0 {
		t.Fatalf("ExtendLease called %d times after NackTask, want 0 (renewal must stop on nack)", n)
	}
}
