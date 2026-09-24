package tasuki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestRequeueWorkflowTask_RenewedClaimStillNacks is the regression test
// for the round-10 P2 (stale local expiry estimate on the delayed-nack
// path): the renewal loop extends the store lease but the local claim
// record stays at the original claim time, so a turn renewed past that
// time that then fails non-contentiously skips its NackTask —
// requeueWorkflowTask sees the original time as expired — and stays
// hidden until lease expiry instead of backing off for a retry delay.
// With the fix each successful renewal refreshes the local estimate and
// the nack is issued.
//
// Without the fix the claim record predates LeaseDuration at requeue
// time (0 NackTask calls); with the fix the renewals kept it fresh
// (1 NackTask call).
func TestRequeueWorkflowTask_RenewedClaimStillNacks(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	store := &nackCountingBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		Queues:        []string{"default"},
		LeaseDuration: 150 * time.Millisecond, // 75ms renewal tick
	})

	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: "renewed-nack-1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: 150 * time.Millisecond, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(claimed))
	}
	task := claimed[0]

	// The tick path records the claim time, then the renewal loop keeps
	// the store lease alive across several ticks — past the original
	// 150ms lease window.
	w.trackWfClaim(task.ID)
	done := make(chan struct{})
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		w.extendLeaseLoop(ctx, task, done, nil, time.Now())
	}()
	// ~75/150/225/300ms ticks all renew successfully (no reclaim); the
	// last refresh lands ~300ms in, keeping the estimate fresh.
	time.Sleep(350 * time.Millisecond)
	close(done)
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal loop did not exit")
	}

	if w.wfLeaseExpired(task.ID) {
		t.Fatal("local lease estimate expired despite successful renewals (refreshWfClaim did not run)")
	}
	w.requeueWorkflowTask(ctx, task, errors.New("injected store failure"))
	if n := store.nackCount(); n != 1 {
		t.Fatalf("NackTask calls=%d, want 1 (renewed worker must nack with a delay, not skip as stale)", n)
	}
}
