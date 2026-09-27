package tasuki_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type countingReleaseBackend struct {
	backend.Backend
	releases atomic.Int32
}

func (b *countingReleaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorker_StaleHandlerDoesNotClearPeerLease covers the Shutdown/handler
// coordination: a stubborn activity ignores cancellation and outlives the
// shutdown grace. Shutdown releases its lease, a peer claims it, then the
// stale invocation returns. Exactly one release may happen and the peer
// lease must survive (backend leases are keyed by task ID alone, so a
// second release would clear the peer's fresh lease and enable duplicate
// execution).
func TestWorker_StaleHandlerDoesNotClearPeerLease(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &countingReleaseBackend{Backend: mem}

	started := make(chan struct{})
	var once atomic.Bool
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		if once.CompareAndSwap(false, true) {
			close(started)
		}
		time.Sleep(600 * time.Millisecond) // ignores cancellation
		return "stale", nil
	}, tasuki.WithName("stubborn"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "stubborn", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("peer-lease-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("activity did not start")
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = w.Shutdown(shCtx)

	// Peer claims the released task.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1", len(peer))
	}

	// Wait for the stale invocation to return well after its 600ms sleep.
	time.Sleep(time.Second)

	if n := store.releases.Load(); n != 1 {
		t.Fatalf("ReleaseLease calls=%d, want exactly 1 (stale handler double-released)", n)
	}
	// Peer lease must still hold: a probe claim finds nothing.
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

// blockingClaimBackend pins activity ClaimTasks until unblock is closed so
// a test can force the ClaimTasks-vs-Shutdown race deterministically. It
// also simulates a pgx-style store that rejects lease releases on a
// canceled context.
type blockingClaimBackend struct {
	backend.Backend
	mu          sync.Mutex
	inFlight    int
	unblock     chan struct{}
	releaseLive atomic.Int32
	releaseDead atomic.Int32
}

func (b *blockingClaimBackend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	if req.Kind != "activity" {
		return b.Backend.ClaimTasks(ctx, req)
	}
	b.mu.Lock()
	b.inFlight++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}()
	select {
	case <-b.unblock:
	case <-time.After(15 * time.Second):
		return nil, context.DeadlineExceeded
	}
	return b.Backend.ClaimTasks(ctx, req)
}

func (b *blockingClaimBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	if ctx.Err() != nil {
		b.releaseDead.Add(1)
		return ctx.Err()
	}
	b.releaseLive.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

func (b *blockingClaimBackend) inFlightClaims() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight
}

// TestWorker_ShutdownRaceClaimReleasedWithLiveContext covers tasks claimed
// concurrently with Shutdown: ClaimTasks returns after stopping=true with
// an already-canceled poll ctx, so trackActivity rejects them. The
// rejection release must use a live detached context (releaseInFlight can
// no longer cover the untracked ID); with a canceled ctx a context-aware
// store rejects the release and the task stalls until lease expiry.
func TestWorker_ShutdownRaceClaimReleasedWithLiveContext(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &blockingClaimBackend{Backend: mem, unblock: make(chan struct{})}

	var calls atomic.Int32
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		calls.Add(1)
		return "x", nil
	}, tasuki.WithName("a"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "a", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	if _, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("race-claim-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)

	// Wait until an activity task exists and the loop is pinned inside an
	// activity ClaimTasks call, so Shutdown is guaranteed to interleave.
	deadline := time.Now().Add(5 * time.Second)
	for {
		counts, err := mem.CountClaimableTasks(ctx, "activity", []string{"default"})
		if err != nil {
			t.Fatal(err)
		}
		if counts["default"] >= 1 && store.inFlightClaims() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("activity task was not claimed concurrently")
		}
		time.Sleep(2 * time.Millisecond)
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Shutdown(shCtx); close(done) }()
	// Shutdown entry (stopping=true + loop cancel) runs synchronously;
	// unblock only after it has definitely happened.
	time.Sleep(300 * time.Millisecond)
	close(store.unblock)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
	}

	if n := store.releaseLive.Load(); n != 1 {
		t.Fatalf("live releases=%d, want 1 (dead=%d)", n, store.releaseDead.Load())
	}
	if n := store.releaseDead.Load(); n != 0 {
		t.Fatalf("canceled-context releases=%d, want 0", n)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("activity executed %d times, want 0 (rejected claim must not run)", n)
	}
	// Released with a live ctx: immediately claimable, no lease-expiry wait.
	probe, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "probe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 1 {
		t.Fatalf("probe claimed %d tasks, want 1 (release was lost)", len(probe))
	}
}

// slowCommitBackend simulates an ordinary store commit slower than a short
// ShutdownReleaseTimeout, rejecting commits on an expired/canceled context
// like pgx Begin does.
type slowCommitBackend struct {
	backend.Backend
	commitOK atomic.Int32
}

func (b *slowCommitBackend) CompleteActivity(ctx context.Context, task backend.Task, ev journal.Event) error {
	time.Sleep(200 * time.Millisecond) // ordinary store latency
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := b.Backend.CompleteActivity(ctx, task, ev); err != nil {
		return err
	}
	b.commitOK.Add(1)
	return nil
}

// TestWorker_NormalCommitIgnoresShutdownReleaseTimeout ensures the
// shutdown-only ShutdownReleaseTimeout does not bound ordinary result
// commits: with a 50ms shutdown bound and 200ms store latency the workflow
// must still complete on the first attempt, not be canceled into
// re-execution.
func TestWorker_NormalCommitIgnoresShutdownReleaseTimeout(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &slowCommitBackend{Backend: mem}

	var calls atomic.Int32
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:           time.Millisecond,
		LeaseDuration:          time.Minute,
		ShutdownReleaseTimeout: 50 * time.Millisecond,
	})
	tasuki.RegisterActivity(w, func(actCtx context.Context, _ struct{}) (string, error) {
		calls.Add(1)
		return "done", nil
	}, tasuki.WithName("quick"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.Execute[struct{}, string](wctx, "quick", struct{}{})
	}, tasuki.WithName("WF"))
	c := tasuki.NewClient(store)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("commit-timeout-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	defer w.Shutdown(ctx)

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
			t.Fatal("workflow failed: ordinary commit was canceled by the shutdown bound")
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow status=%q, want completed (ordinary commit canceled?)", info.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("activity executed %d times, want exactly once (no re-execution)", n)
	}
	if n := store.commitOK.Load(); n != 1 {
		t.Fatalf("successful commits=%d, want 1", n)
	}
}
