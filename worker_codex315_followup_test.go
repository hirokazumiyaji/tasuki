package tasuki_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// slowExtendBackend simulates a slow store: ExtendLease stays in flight
// long enough to overlap the nack, detecting renewal that is not joined
// before NackTask and overwrites its visible_at with the lease duration.
type slowExtendBackend struct {
	backend.Backend
	nacked   chan struct{}
	nackOnce sync.Once
}

func (b *slowExtendBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	time.Sleep(400 * time.Millisecond)
	return b.Backend.ExtendLease(ctx, taskID, d)
}

func (b *slowExtendBackend) NackTask(ctx context.Context, t backend.Task, delay time.Duration) error {
	err := b.Backend.NackTask(ctx, t, delay)
	b.nackOnce.Do(func() { close(b.nacked) })
	return err
}

// slowHeadBackend delays workflow-state loads so the nacking handler is
// slow enough for renewal to be inside ExtendLease when the nack lands.
type slowHeadBackend struct {
	backend.Backend
}

func (b *slowHeadBackend) LoadWorkflowHead(ctx context.Context, instanceID string) (*backend.WorkflowState, error) {
	time.Sleep(200 * time.Millisecond)
	return b.Backend.LoadWorkflowHead(ctx, instanceID)
}

// TestWorkflow_NackJoinsInFlightRenewal is a regression test for the
// join finding: closing the renewal done channel does not wait for
// extendLeaseLoop, which may be inside ExtendLease and overwrite
// NackTask's visible_at (IncompatibleRetryDelay) with the lease duration
// after the nack lands. Renewal must be stopped AND joined before the
// nack, so the delay a compatible worker observes stays intact.
//
// The worker is shut down right after the first nack, so no re-claim can
// mask the outcome: the probe observes exactly one nack racing one
// in-flight ExtendLease. With the fix the nack (joined, post-extend)
// stands; without it the extend lands after and the short lease expires
// before the probe.
func TestWorkflow_NackJoinsInFlightRenewal(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	t0 := time.Now().UTC()
	mem.SetNow(t0)
	slow := &slowExtendBackend{Backend: mem, nacked: make(chan struct{})}
	spy := &slowHeadBackend{Backend: slow}

	// Advance the store clock at wall speed so the overwrite (lease
	// expiry) becomes observable to a peer probe.
	stopAdv := make(chan struct{})
	var advWg sync.WaitGroup
	advWg.Add(1)
	go func() {
		defer advWg.Done()
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopAdv:
				return
			case <-tk.C:
				mem.SetNow(mem.Now().Add(10 * time.Millisecond))
			}
		}
	}()
	defer func() { close(stopAdv); advWg.Wait() }()

	c := tasuki.NewClient(spy)
	if _, err := tasuki.Start(ctx, c, "ghostWF", struct{}{}, tasuki.WithID("nack-join-ghost")); err != nil {
		t.Fatal(err)
	}
	w := tasuki.NewWorker(spy, tasuki.WorkerOptions{
		PollInterval:           5 * time.Millisecond,
		LeaseDuration:          30 * time.Millisecond,
		WorkflowConcurrency:    1,
		ClaimLimit:             1,
		IncompatibleRetryDelay: 5 * time.Second,
		WorkerID:               "w1",
	})
	// NOTE: ghostWF is deliberately unregistered so its task is nacked, and
	// the state load is slowed (200ms) so renewal is inside the slow
	// ExtendLease (400ms) when the nack lands — the in-flight-overwrite
	// interleaving the join must prevent.
	w.Start(ctx)
	// Wait for the first nack, then shut down immediately: no further
	// claims or nacks may run, so the probe below observes exactly the
	// outcome of one nack racing one in-flight ExtendLease.
	select {
	case <-slow.nacked:
	case <-time.After(5 * time.Second):
		t.Fatal("first nack did not happen")
	}
	if err := w.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// Let any in-flight ExtendLease land (400ms), then probe. Without the
	// join it overwrites the 5s nack delay with the 30ms lease, which has
	// expired by now; with the join the nack stands.
	time.Sleep(600 * time.Millisecond)

	// The nacked task must still be hidden by its 5s delay. An unjoined
	// ExtendLease would have replaced the delay with the 30ms lease,
	// making the task claimable here.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 0 {
		t.Fatalf("peer claimed %d tasks, want 0 (in-flight renewal overwrote the nack delay)", len(peer))
	}
}
