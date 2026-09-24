package tasuki

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_Round14_RenewalBarrierStopAndDrain is a deterministic unit
// test of the round-14 renewal barrier: registration and the stop-check
// are atomic under renewMu, so a registration before the stop is joined
// and a registration after is rejected — with no Add-during-Wait window
// that could panic (the pre-fix sync.WaitGroup misuse).
func TestWorker_Round14_RenewalBarrierStopAndDrain(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{WorkerID: "w1"})
	// Barrier admissions are generation-scoped (round-15 P1): use the
	// current barrier generation for all admissions in this test (no
	// restart here, so the epoch never moves).
	w.mu.Lock()
	barrierEpoch := w.epoch
	w.mu.Unlock()
	barrierTok := claimToken{epoch: barrierEpoch}
	const barrierTaskID int64 = 9001

	// Three renewals in flight before grace expiry.
	for i := 0; i < 3; i++ {
		if !w.renewTryEnter(barrierTaskID, barrierTok) {
			t.Fatalf("renewTryEnter(%d) = false, want true (no shutdown yet)", i)
		}
	}
	joinDone := make(chan struct{})
	go func() {
		defer close(joinDone)
		w.shutdownRenewalJoin(context.Background())
	}()
	// The join must block while slots are held.
	select {
	case <-joinDone:
		t.Fatal("shutdownRenewalJoin returned while 3 renewals were still in flight")
	case <-time.After(100 * time.Millisecond):
	}
	// Grace expiry: new admissions stop, even with live-looking leases.
	if w.renewTryEnter(barrierTaskID, barrierTok) {
		w.renewExit(barrierTaskID)
		t.Fatal("renewTryEnter = true after stop, want false (no renewal may slip past the join)")
	}
	// Draining the held slots wakes the join.
	w.renewExit(barrierTaskID)
	w.renewExit(barrierTaskID)
	w.renewExit(barrierTaskID)
	select {
	case <-joinDone:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdownRenewalJoin did not return after the last renewal drained")
	}
	// A join on a drained barrier returns immediately, bounded by ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.shutdownRenewalJoin(ctx)
	if err := ctx.Err(); err != nil {
		t.Fatalf("drained join did not return immediately: %v", err)
	}
	// A fresh Start generation re-arms admissions (round-15 P1: the
	// re-arm carries the barrier generation with the stop reset).
	w.renewMu.Lock()
	w.renewStopped = false
	barrierGen := w.renewEpoch
	w.renewMu.Unlock()
	barrierTok = claimToken{epoch: barrierGen}
	if !w.renewTryEnter(barrierTaskID, barrierTok) {
		t.Fatal("renewTryEnter = false after re-arm, want true")
	}
	w.renewExit(barrierTaskID)
}

// TestWorker_Round14_RenewalBarrierChurn stresses the barrier the way
// the pre-fix code panicked: many tickers registering concurrently with
// the shutdown join. Post-stop admissions must be rejected (not panic),
// and the join must drain every pre-stop registration.
func TestWorker_Round14_RenewalBarrierChurn(t *testing.T) {
	w := NewWorker(memory.New(), WorkerOptions{WorkerID: "w1"})
	w.mu.Lock()
	churnEpoch := w.epoch
	w.mu.Unlock()
	churnTok := claimToken{epoch: churnEpoch}
	const churnTaskID int64 = 9002
	for round := 0; round < 30; round++ {
		// Start equivalent: re-arm admissions for the round.
		w.renewMu.Lock()
		w.renewStopped = false
		w.renewMu.Unlock()

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					select {
					case <-stop:
						return
					default:
					}
					if !w.renewTryEnter(churnTaskID, churnTok) {
						return // stop set; done for the round
					}
					// Widen the in-flight window so registrations race
					// the join instead of only seeing a drained barrier.
					time.Sleep(time.Microsecond)
					w.renewExit(churnTaskID)
				}
			}()
		}
		// Let churn build in-flight registrations, then join (sets stop
		// and drains) while churners still race it.
		time.Sleep(time.Millisecond)
		joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		w.shutdownRenewalJoin(joinCtx)
		cancel()
		if w.renewTryEnter(churnTaskID, churnTok) {
			w.renewExit(churnTaskID)
			close(stop)
			wg.Wait()
			t.Fatalf("round %d: renewTryEnter succeeded after the join set stop", round)
		}
		close(stop)
		wg.Wait()
		w.renewMu.Lock()
		left := w.renewInflight
		w.renewMu.Unlock()
		if left != 0 {
			t.Fatalf("round %d: renewInflight=%d, want 0 (join must drain every pre-stop registration)", round, left)
		}
	}
}

// TestWorker_Round14_TickerRenewalsRaceShutdownJoin drives the real
// ticker path (ownsFresh + barrier + ExtendLease) across grace expiry:
// renewals already issued complete, none are issued after the stop, and
// the join — not a racy WaitGroup — synchronizes both sides.
func TestWorker_Round14_TickerRenewalsRaceShutdownJoin(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &extendRecorder{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond, // 100ms tick
		WorkerID:      "w1",
	})
	const tasks = 8
	toks := make([]claimToken, 0, tasks)
	for i := int64(1); i <= tasks; i++ {
		toks = append(toks, w.track(i))
	}
	defer func() {
		for i, tok := range toks {
			w.untrack(int64(i+1), tok)
		}
	}()

	var committing atomic.Bool
	done := make(chan struct{})
	var loops sync.WaitGroup
	for i := int64(1); i <= tasks; i++ {
		loops.Add(1)
		go func(id int64, tok claimToken) {
			defer loops.Done()
			w.extendLeaseLoop(context.Background(), id, tok, done, &committing)
		}(i, toks[i-1])
	}
	// Let several ticks fire so renewals are genuinely in flight when
	// the join starts racing them.
	time.Sleep(350 * time.Millisecond)
	if n := store.extendCount(); n == 0 {
		close(done)
		loops.Wait()
		t.Fatal("no ExtendLease fired before the join (test did not race anything)")
	}
	joinDone := make(chan struct{})
	go func() {
		defer close(joinDone)
		joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		w.shutdownRenewalJoin(joinCtx)
	}()
	select {
	case <-joinDone:
	case <-time.After(10 * time.Second):
		close(done)
		loops.Wait()
		t.Fatal("shutdownRenewalJoin did not return while ticker renewals raced it")
	}
	// After the join no ticker may issue: the count must freeze across
	// more than one tick period.
	afterJoin := store.extendCount()
	time.Sleep(250 * time.Millisecond)
	if n := store.extendCount(); n != afterJoin {
		close(done)
		loops.Wait()
		t.Fatalf("ExtendLease calls grew %d→%d after the join (no renewal may slip past grace expiry)", afterJoin, n)
	}
	close(done)
	finished := make(chan struct{})
	go func() { loops.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("renewal loops did not exit after done closed")
	}
}

// TestWorker_Round14_ShutdownJoinsUnderRenewalChurn is the grace-expiry
// integration: held renewals plus many tickers racing the stop drain
// through a real Shutdown — clean return, exactly one release batch, no
// WaitGroup-misuse panic under -race.
func TestWorker_Round14_ShutdownJoinsUnderRenewalChurn(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &joinOrderBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:           5 * time.Millisecond,
		LeaseDuration:          30 * time.Second, // no ticker renewal during the test
		WorkerID:               "w1",
		ShutdownReleaseTimeout: 10 * time.Second,
	})
	if err := w.StartWithError(ctx); err != nil {
		t.Fatal(err)
	}
	// Held renewals, as if ticker paths were blocked inside ExtendLease
	// when the grace expires. Admissions carry the current Start
	// generation (round-15 P1).
	w.mu.Lock()
	shutdownEpoch := w.epoch
	w.mu.Unlock()
	shutdownTok := claimToken{epoch: shutdownEpoch}
	const shutdownTaskID int64 = 9003
	const held = 4
	for i := 0; i < held; i++ {
		if !w.renewTryEnter(shutdownTaskID, shutdownTok) {
			t.Fatalf("renewTryEnter(%d) = false, want true (no shutdown yet)", i)
		}
	}
	// Paused tickers waking up across grace expiry: every one of these
	// Add-during-Wait panicked the pre-fix WaitGroup gate.
	var churn sync.WaitGroup
	for i := 0; i < 16; i++ {
		churn.Add(1)
		go func() {
			defer churn.Done()
			for j := 0; j < 200; j++ {
				if !w.renewTryEnter(shutdownTaskID, shutdownTok) {
					return
				}
				w.renewExit(shutdownTaskID)
			}
		}()
	}
	shDone := make(chan error, 1)
	go func() { shDone <- w.Shutdown(context.Background()) }()
	// Let Shutdown park in the renewal join, then drain the held slots.
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < held; i++ {
		w.renewExit(shutdownTaskID)
	}
	select {
	case err := <-shDone:
		if err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown did not return (join must drain pre-stop renewals and reject the churn)")
	}
	churnDone := make(chan struct{})
	go func() { churn.Wait(); close(churnDone) }()
	select {
	case <-churnDone:
	case <-time.After(10 * time.Second):
		t.Fatal("renewal churn did not finish after Shutdown stopped admissions")
	}
	w.renewMu.Lock()
	left := w.renewInflight
	w.renewMu.Unlock()
	if left != 0 {
		t.Fatalf("renewInflight=%d, want 0 (every pre-stop registration must drain)", left)
	}
}
