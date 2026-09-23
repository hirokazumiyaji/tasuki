package tasuki

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// errGateCtx is a hook context that freezes handleActivity inside the
// post-invocation cancel check. The first Err call after the activity
// function has returned blocks until the test releases it, so the test can
// deliver a Start-parent cancel (or shutdown-grace expiry) at an exact
// point in the check-then-commit sequence instead of relying on timing.
// Done is test-controlled; Err always reports nil once released.
type errGateCtx struct {
	context.Context
	returned   *atomic.Bool
	doneCh     chan struct{}
	errBlocked chan struct{}
	errRelease chan struct{}
	blockOnce  atomic.Bool
}

func (c *errGateCtx) Done() <-chan struct{} { return c.doneCh }

func (c *errGateCtx) Err() error {
	if c.returned.Load() && c.blockOnce.CompareAndSwap(false, true) {
		close(c.errBlocked)
		<-c.errRelease
	}
	return nil
}

// gateCommitBackend pins CompleteActivity on a test-controlled gate once
// armed, so the test can hold a detached result commit past the lease.
type gateCommitBackend struct {
	backend.Backend
	armCommit     atomic.Bool
	commitOnce    atomic.Bool
	commitEntered chan struct{}
	commitRelease chan struct{}
}

func (b *gateCommitBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	if b.armCommit.Load() {
		if b.commitOnce.CompareAndSwap(false, true) {
			close(b.commitEntered)
		}
		<-b.commitRelease
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

// gateNackBackend pins NackTask on a test-controlled gate once armed, so
// the test can hold a detached incompatible-activity nack past the lease.
type gateNackBackend struct {
	backend.Backend
	armNack     atomic.Bool
	nackOnce    atomic.Bool
	nackEntered chan struct{}
	nackRelease chan struct{}
}

func (b *gateNackBackend) NackTask(ctx context.Context, task backend.Task, delay time.Duration) error {
	if b.armNack.Load() {
		if b.nackOnce.CompareAndSwap(false, true) {
			close(b.nackEntered)
		}
		<-b.nackRelease
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return b.Backend.NackTask(ctx, task, delay)
}

// setupClaimableActivityTask starts a workflow executing activityName while
// the activity is still unregistered, drives the workflow forward with
// PollOnce (the unregistered-activity nack leaves the task visible), and
// returns the claimable activity task for a direct handleActivity run.
func setupClaimableActivityTask(t *testing.T, ctx context.Context, store backend.Backend, mem *memory.Backend, w *Worker, instanceID, activityName string) backend.Task {
	t.Helper()
	c := NewClient(store)
	RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, activityName, struct{}{})
	}, WithName("WF"))
	if _, err := Start(ctx, c, "WF", struct{}{}, WithID(instanceID)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.PollOnce(ctx)
		tasks, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "activity", Queues: []string{"default"}, Limit: 10,
			Lease: 200 * time.Millisecond, WorkerID: "setup",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) == 1 {
			return tasks[0]
		}
		if len(tasks) > 1 {
			t.Fatalf("setup claimed %d activity tasks, want 1", len(tasks))
		}
		if time.Now().After(deadline) {
			t.Fatal("activity task never became claimable")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func probeActivityTasks(t *testing.T, ctx context.Context, mem *memory.Backend) []backend.Task {
	t.Helper()
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	return peer
}

// TestWorker_DetachedCommitRenewalEnteredBeforeCancelCheck covers the
// atomic-entry race: a Start-parent cancel (or shutdown-grace expiry)
// landing between the post-invocation ctx check and the commit must not
// leave a detached commit running without lease renewal. The hook context
// freezes the handler at the check, so the cancel lands deterministically
// in that window: with the flag set before the check, renewal observes it
// and keeps the lease alive through the gated commit; with the old
// check-then-set order the renewal loop exits and a peer reclaims the
// task mid-commit.
func TestWorker_DetachedCommitRenewalEnteredBeforeCancelCheck(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &gateCommitBackend{
		Backend:       mem,
		commitEntered: make(chan struct{}),
		commitRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "atomic-commit-1", "hooked")

	var returned atomic.Bool
	var calls atomic.Int32
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		calls.Add(1)
		returned.Store(true)
		return "ok", nil
	}, WithName("hooked"))

	hook := &errGateCtx{
		Context:    context.Background(),
		returned:   &returned,
		doneCh:     make(chan struct{}),
		errBlocked: make(chan struct{}),
		errRelease: make(chan struct{}),
	}
	w.track(task.ID)
	defer w.untrack(task.ID)
	store.armCommit.Store(true)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(hook, task) }()

	select {
	case <-hook.errBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach the post-invocation cancel check")
	}
	time.Sleep(50 * time.Millisecond) // renewal loop parked in select
	close(hook.doneCh)                // cancel lands inside the check window
	time.Sleep(200 * time.Millisecond)
	close(hook.errRelease) // check passes; the detached commit starts
	select {
	case <-store.commitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached commit did not start")
	}

	// Hold the commit past the original lease: without renewal covering
	// the commit, a peer reclaims the task here and the task-ID-only
	// CompleteActivity would clobber the peer's task.
	mem.SetNow(t0.Add(10 * time.Second))
	time.Sleep(300 * time.Millisecond)
	if peer := probeActivityTasks(t, ctx, mem); len(peer) != 0 {
		close(store.commitRelease)
		<-herrCh
		t.Fatalf("peer claimed %d tasks mid-commit, want 0 (renewal must be entered before the cancel check)", len(peer))
	}
	close(store.commitRelease)
	select {
	case herr := <-herrCh:
		if herr != nil {
			t.Fatalf("handleActivity = %v, want nil", herr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the commit was released")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("activity executed %d times, want exactly once (no duplicate execution)", n)
	}
}

// TestWorker_UnregisteredActivityRenewalCoversEarlyNack covers the early
// result paths: the unregistered-activity nack must run under lease
// renewal too. Without renewal starting before the registry lookup, the
// detached nack runs uncovered and a peer reclaims the task mid-nack.
func TestWorker_UnregisteredActivityRenewalCoversEarlyNack(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &gateNackBackend{
		Backend:     mem,
		nackEntered: make(chan struct{}),
		nackRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	// "ghost" is never registered: the setup nack leaves its task
	// visible, and the gated run below exercises the same early path.
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "early-nack-1", "ghost")

	w.track(task.ID)
	defer w.untrack(task.ID)
	store.armNack.Store(true)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(ctx, task) }()
	select {
	case <-store.nackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("detached nack did not start")
	}

	mem.SetNow(t0.Add(10 * time.Second))
	time.Sleep(300 * time.Millisecond)
	if peer := probeActivityTasks(t, ctx, mem); len(peer) != 0 {
		close(store.nackRelease)
		<-herrCh
		t.Fatalf("peer claimed %d tasks mid-nack, want 0 (renewal must cover early result paths)", len(peer))
	}
	close(store.nackRelease)
	select {
	case herr := <-herrCh:
		if herr != nil {
			t.Fatalf("handleActivity = %v, want nil", herr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after the nack was released")
	}
}
