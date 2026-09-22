package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// commitGateBackend pins CompleteActivity so a test can force Shutdown to
// interleave with a detached result commit, and counts ReleaseLease calls.
type commitGateBackend struct {
	backend.Backend
	entered   chan struct{}
	enterOnce atomic.Bool
	releases  atomic.Int32
}

func (b *commitGateBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	if b.enterOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	time.Sleep(400 * time.Millisecond) // commit underway while Shutdown expires
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

func (b *commitGateBackend) ReleaseLease(ctx context.Context, id int64) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, id)
}

// TestWorker_ShutdownDuringCommitDoesNotRelease covers the shutdown-expiry /
// detached-commit race: Shutdown's context expires while CompleteActivity is
// still running on its detached commit context. releaseInFlight must not
// hand the committing task to a peer (the old task-ID-only commit would then
// delete or reschedule the peer-owned task). The commit must still land and
// the activity must run exactly once.
func TestWorker_ShutdownDuringCommitDoesNotRelease(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &commitGateBackend{Backend: mem, entered: make(chan struct{})}

	var calls atomic.Int32
	newActivity := func() func(context.Context, struct{}) (string, error) {
		return func(context.Context, struct{}) (string, error) {
			calls.Add(1)
			return "ok", nil
		}
	}
	newWorkflow := func() func(*workflow.Context, struct{}) (string, error) {
		return func(wctx *workflow.Context, _ struct{}) (string, error) {
			return workflow.Execute[struct{}, string](wctx, "gated", struct{}{})
		}
	}

	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, newActivity(), tasuki.WithName("gated"))
	tasuki.RegisterWorkflow(w, newWorkflow(), tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("shutdown-commit-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not enter CompleteActivity")
	}

	// Expire Shutdown while the detached commit is still running.
	shCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.Shutdown(shCtx)

	if n := store.releases.Load(); n != 0 {
		t.Fatalf("ReleaseLease calls=%d, want 0 (committing task must not be released)", n)
	}
	// Lease still held (or task already gone): a peer must not find it claimable.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 0 {
		t.Fatalf("peer claimed %d tasks, want 0 (commit in progress must hold the lease)", len(peer))
	}

	// The detached commit must still land: drain with a fresh worker.
	w2 := tasuki.NewWorker(store, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w2, newActivity(), tasuki.WithName("gated"))
	tasuki.RegisterWorkflow(w2, newWorkflow(), tasuki.WithName("WF"))
	w2.Start(ctx)
	defer w2.Shutdown(ctx)
	deadline := time.Now().Add(8 * time.Second)
	for {
		info, err := c.Get(ctx, h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == tasuki.StatusCompleted {
			break
		}
		if info.Status == tasuki.StatusFailed {
			t.Fatal("workflow failed: detached commit was lost")
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow status=%q, want completed (releases=%d)", info.Status, store.releases.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("activity executed %d times, want exactly once (no duplicate execution)", n)
	}
}

// TestWorker_ParentCancelStaleReleaseDoesNotClearPeerLease covers the
// Start-parent cancellation path: the parent is canceled without Shutdown,
// renewal stops, and a cancel-ignoring activity keeps running past
// LeaseDuration. A peer reclaims the expired lease before the stale
// invocation returns; the stale ReleaseLease must be skipped (backend leases
// are keyed by task ID alone, so it would clear the peer's fresh lease).
func TestWorker_ParentCancelStaleReleaseDoesNotClearPeerLease(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &countingReleaseBackend{Backend: mem}

	started := make(chan struct{})
	var once atomic.Bool
	done := make(chan struct{})
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: 150 * time.Millisecond,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		if once.CompareAndSwap(false, true) {
			close(started)
		}
		time.Sleep(600 * time.Millisecond) // ignores cancellation
		close(done)
		return "stale", nil
	}, tasuki.WithName("stubborn"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "stubborn", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("parent-cancel-lease-1")); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(ctx)
	w.Start(parent)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("activity did not start")
	}

	// Cancel the Start parent without Shutdown: renewal stops here.
	cancel()
	// Let the store lease expire and be reclaimed by a peer. The memory
	// store clock is frozen, so advance it explicitly.
	mem.SetNow(t0.Add(10 * time.Second))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1 (lease must expire after parent cancel)", len(peer))
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stale activity did not return")
	}
	// Let the handler's post-return release path run (mutex-only when skipped).
	time.Sleep(300 * time.Millisecond)

	if n := store.releases.Load(); n != 0 {
		t.Fatalf("ReleaseLease calls=%d, want 0 (stale post-expiry release must be skipped)", n)
	}
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "probe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 0 {
		t.Fatalf("probe claimed %d tasks, want 0 (peer lease was cleared)", len(probe))
	}
}
