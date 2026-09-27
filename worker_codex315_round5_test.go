package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// countReleaseBackend counts ReleaseLease calls while delegating to memory.
type countReleaseBackend struct {
	backend.Backend
	releases atomic.Int32
}

func (b *countReleaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorkflow_ShutdownTimeoutLateCancelReleasesOnce is a regression test for
// the shutdown-timeout double release: Shutdown times out waiting for a
// blocked turn and releaseInFlight releases the task; a peer re-claims it;
// the old turn then observes the cancel and must NOT release again, since
// backends match the lease by task ID/key alone and a second release would
// clear the peer's fresh lease, letting a third worker execute concurrently
// with the peer.
//
// Interleaving (deterministic, no timing dependence):
//  1. worker A claims the task; its turn blocks inside the workflow function.
//  2. Shutdown with an expired grace releases the lease (release #1).
//  3. a peer claims the task while the old turn is still blocked.
//  4. the old turn unblocks, observes the canceled tick context, and takes
//     the abandon path.
// With the fix the abandon path funnels through in-flight ownership, finds
// the entry already taken by releaseInFlight, and skips the second release:
// exactly 1 ReleaseLease lands and the peer's lease stays intact (a third
// claim finds nothing). Without the fix the abandon path releases
// unconditionally: 2 releases land and the peer's lease is cleared (a third
// claim succeeds).
func TestWorkflow_ShutdownTimeoutLateCancelReleasesOnce(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &countReleaseBackend{Backend: mem}

	entered := make(chan struct{})
	unblock := make(chan struct{})
	var enterOnce atomic.Bool
	w := tasuki.NewWorker(store, tasuki.WorkerOptions{
		PollInterval:        5 * time.Millisecond,
		LeaseDuration:       time.Minute,
		WorkflowConcurrency: 1,
		ClaimLimit:          1,
		WorkerID:            "w1",
	})
	tasuki.RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		if enterOnce.CompareAndSwap(false, true) {
			close(entered)
		}
		<-unblock
		return "late", nil
	}, tasuki.WithName("BLOCK"))
	c := tasuki.NewClient(store)
	if _, err := tasuki.Start(ctx, c, "BLOCK", struct{}{}, tasuki.WithID("shutdown-fence-1")); err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow turn did not start")
	}

	// Shutdown with a grace shorter than the blocked turn: the wait times
	// out while the turn is still tracked, so releaseInFlight releases it.
	shCtx, shCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer shCancel()
	shutDone := make(chan error, 1)
	go func() { shutDone <- w.Shutdown(shCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for store.releases.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := store.releases.Load(); got != 1 {
		close(unblock)
		<-shutDone
		t.Fatalf("shutdown releases=%d, want 1", got)
	}

	// Peer re-claims the released task while the old turn is still blocked.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		close(unblock)
		<-shutDone
		t.Fatalf("peer claimed %d tasks, want 1", len(peer))
	}

	// Let the old turn observe the cancel and take the abandon path, then
	// wait for Shutdown and the loop to drain.
	close(unblock)
	select {
	case <-shutDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	time.Sleep(500 * time.Millisecond)

	if got := store.releases.Load(); got != 1 {
		t.Fatalf("ReleaseLease calls=%d, want exactly 1 (late cancel must not release the peer-owned lease)", got)
	}
	third, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third worker claimed %d tasks, want 0 (peer lease must stay intact)", len(third))
	}
}
