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

// gateFlushBackend pins workflow commits on a test-controlled gate so a
// test can run releaseInFlight while a leased, uncommitted advancement is
// still pending, and counts ReleaseLease calls.
type gateFlushBackend struct {
	backend.Backend
	entered   chan struct{}
	enterOnce sync.Once
	release   chan struct{}
	releases  atomic.Int32
}

func (b *gateFlushBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *gateFlushBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	if batcher, ok := b.Backend.(backend.AdvancementBatcher); ok {
		return batcher.CommitAdvancements(ctx, advs)
	}
	for _, adv := range advs {
		if err := b.Backend.CommitAdvancement(ctx, adv); err != nil {
			return err
		}
	}
	return nil
}

func (b *gateFlushBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorkflow_PendingCommitStaysTrackedThroughFlush is a regression test
// for the pending-tracking finding: a pending advancement's task was
// removed from inFlight while still leased and uncommitted, so a shutdown
// in the flush window found nothing to release (and the canceled flush is
// rejected by context-aware backends), leaving peers waiting for lease
// expiry. Pending tasks must stay tracked until the commit succeeds.
func TestWorkflow_PendingCommitStaysTrackedThroughFlush(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &gateFlushBackend{Backend: mem, entered: make(chan struct{}), release: make(chan struct{})}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(store)
	h, err := Start(ctx, c, "WF", struct{}{}, WithID("pending-track-1"))
	if err != nil {
		t.Fatal(err)
	}

	tickDone := make(chan struct{})
	go func() { w.tickWorkflows(ctx); close(tickDone) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not start")
	}

	// Shutdown window: the pending commit is leased but uncommitted, so it
	// must still be tracked and released for a peer.
	w.releaseInFlight(context.Background())
	if n := store.releases.Load(); n != 1 {
		close(store.release)
		<-tickDone
		t.Fatalf("ReleaseLease calls=%d, want 1 (pending commit must stay tracked through flush)", n)
	}
	close(store.release)
	select {
	case <-tickDone:
	case <-time.After(5 * time.Second):
		t.Fatal("tick did not finish")
	}

	// The commit still lands after the release: the task delete is by ID.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusCompleted {
		t.Fatalf("status=%s, want completed", info.Status)
	}
}

// captureReleaseBackend records the full task identity of every release.
type captureReleaseBackend struct {
	backend.Backend
	mu  sync.Mutex
	got []backend.Task
}

func (b *captureReleaseBackend) ReleaseLease(_ context.Context, t backend.Task) error {
	b.mu.Lock()
	b.got = append(b.got, t)
	b.mu.Unlock()
	return nil
}

func (b *captureReleaseBackend) released() []backend.Task {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]backend.Task(nil), b.got...)
}

// TestReleaseInFlightKeepsFullTaskIdentity is a regression test for the
// shutdown-release routing finding: the abandon path passed only the
// numeric ID to ReleaseLease, which addresses a missing ACT# key for WF#
// workflow tasks on DynamoDB/Firestore (ErrNotFound). Releases must carry
// the full task identity so backends can route by kind.
func TestReleaseInFlightKeepsFullTaskIdentity(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &captureReleaseBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{Queues: []string{"default"}})

	want := backend.Task{ID: 7, Kind: "workflow", Queue: "default", InstanceID: "wf-1", WorkerID: "w1", Attempt: 1}
	w.track(want)
	w.releaseInFlight(ctx)
	got := store.released()
	if len(got) != 1 {
		t.Fatalf("releases=%d, want 1", len(got))
	}
	if got[0].ID != want.ID || got[0].Kind != "workflow" || got[0].InstanceID != "wf-1" {
		t.Fatalf("released task=%+v, want workflow wf-1 id 7 (kind-routed release)", got[0])
	}

	// releaseWorkflowLease (shutdown abandon path) routes the same way.
	store2 := &captureReleaseBackend{Backend: mem}
	w2 := NewWorker(store2, WorkerOptions{Queues: []string{"default"}})
	w2.releaseWorkflowLease(want)
	got2 := store2.released()
	if len(got2) != 1 {
		t.Fatalf("abandon releases=%d, want 1", len(got2))
	}
	if got2[0].Kind != "workflow" || got2[0].InstanceID != "wf-1" {
		t.Fatalf("abandon released task=%+v, want kind-routed workflow task", got2[0])
	}
}

// TestAcceptLocalResultPreservesOnTimeResult is a regression test for the
// local-deadline finding: a result sent to the buffered channel before the
// deadline but consumed after had ctx.Err() != nil at consumption and was
// discarded as a timeout. Completion-before-expiry is now recorded at
// production time (onTime) and preserved.
func TestAcceptLocalResultPreservesOnTimeResult(t *testing.T) {
	const timeout = 50 * time.Millisecond

	// On-time success consumed after expiry: production-time flag wins.
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond) // let the deadline pass deterministically
	if expired.Err() == nil {
		t.Fatal("test setup: acceptance context should be expired")
	}
	out, err := acceptLocalResult("fast", callResult{out: []byte("ok"), onTime: true}, expired, context.Background(), timeout)
	if err != nil || string(out) != "ok" {
		t.Fatalf("on-time success discarded (out=%q err=%v), want preserved", out, err)
	}

	// On-time activity error is preserved too, not mapped to a timeout.
	actErr := errors.New("boom")
	_, err = acceptLocalResult("fast", callResult{err: actErr, onTime: true}, expired, context.Background(), timeout)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("on-time activity error mapped (err=%v), want the activity error", err)
	}

	// Late success is still discarded as a timeout, never journaled.
	_, err = acceptLocalResult("late", callResult{out: []byte("late-ok")}, expired, context.Background(), timeout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late success accepted (err=%v), want deadline exceeded", err)
	}

	// Late failure is still a timeout, never the late payload.
	_, err = acceptLocalResult("late", callResult{err: errors.New("late-boom")}, expired, context.Background(), timeout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late failure accepted (err=%v), want deadline exceeded", err)
	}

	// Shutdown cancellation still surfaces the turn error for late results.
	runCtx, stop := context.WithCancel(context.Background())
	stop()
	expired2, cancel2 := context.WithTimeout(runCtx, time.Nanosecond)
	defer cancel2()
	time.Sleep(10 * time.Millisecond)
	_, err = acceptLocalResult("x", callResult{out: []byte("v")}, expired2, runCtx, timeout)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want turn cancellation", err)
	}

	// Live acceptance passes results and activity errors through.
	live, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()
	if out, err := acceptLocalResult("ok", callResult{out: []byte("v"), onTime: true}, live, context.Background(), timeout); err != nil || string(out) != "v" {
		t.Fatalf("live success: out=%q err=%v", out, err)
	}
}
