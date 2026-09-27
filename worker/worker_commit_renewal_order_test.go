package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// commitOrderBackend extends joinOrderBackend (ExtendLease/ReleaseLease
// order recording) with a CompleteActivity counter, so a test can assert
// that no result commit ran after ownership was lost.
type commitOrderBackend struct {
	*joinOrderBackend
	completes atomic.Int32
}

func (b *commitOrderBackend) CompleteActivity(ctx context.Context, task backend.Task, ev journal.Event) error {
	b.completes.Add(1)
	return b.joinOrderBackend.Backend.CompleteActivity(ctx, task, ev)
}

// slowClaimBackend delays every ClaimTasks call by a fixed latency so the
// test can separate the pre-claim instant from the post-claim return.
type slowClaimBackend struct {
	backend.Backend
	delay time.Duration
}

func (b *slowClaimBackend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	time.Sleep(b.delay)
	return b.Backend.ClaimTasks(ctx, req)
}

// sleepExtendBackend delays every ExtendLease call by a fixed latency so
// the test can separate the pre-renewal instant from the post-renewal
// return.
type sleepExtendBackend struct {
	backend.Backend
	delay time.Duration
}

func (b *sleepExtendBackend) ExtendLease(ctx context.Context, task backend.Task, d time.Duration) error {
	time.Sleep(b.delay)
	return b.Backend.ExtendLease(ctx, task, d)
}

// TestWorker_CommitEntryOrdersRenewalBeforeRelease is a regression test
// for the renewal-before-release ordering finding: a grace-cancel landing
// after the handler's live-context check but before the commit ownership
// transfer lets Shutdown's releaseInFlight remove and release the task.
// The commit transfer must then fail WITHOUT any detached renewal having
// started — joining an already-running renewal after the release cannot
// order it before that earlier ReleaseLease, and the renewal lands after,
// re-hiding the task (or modifying a peer's fresh lease).
//
// Interleaving (deterministic): the hook context freezes the handler
// between invoke-return and the commit transfer while reporting a live
// context (commit path, not release path); the test then cancels the
// renewal loop's context and runs the real releaseInFlight (the preceding
// ReleaseLease the previous test lacked) before releasing the handler
// into the commit transfer. With the fix detached mode is entered
// atomically with the transfer, so the cancel finds the flag still clear,
// the loop exits without issuing, the transfer fails, and no commit runs:
// no extend completes after the release and the released task stays
// peer-claimable. Without the fix the flag is already set when the cancel
// lands, the loop is inside a detached ExtendLease when the release lands,
// and the renewal completes after the release, re-hiding the task.
func TestWorker_CommitEntryOrdersRenewalBeforeRelease(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	joiner := &joinOrderBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendGate:    make(chan struct{}),
	}
	store := &commitOrderBackend{joinOrderBackend: joiner}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          10 * time.Second, // 5s tick: no periodic renewal during the test
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "commit-order-1", "hooked")

	var returned atomic.Bool
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		returned.Store(true)
		return "ok", nil
	}, WithName("hooked"))

	// Live-context freeze: Err blocks once after invoke returns, then
	// reports nil, so the handler proceeds toward the commit transfer
	// (not the release path) once released.
	hook := &errGateCtx{
		Context:    context.Background(),
		returned:   &returned,
		doneCh:     make(chan struct{}),
		errBlocked: make(chan struct{}),
		errRelease: make(chan struct{}),
	}
	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(hook, task, tok) }()

	select {
	case <-hook.errBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach the post-invocation check")
	}
	time.Sleep(50 * time.Millisecond) // renewal loop parked in select
	store.armBlock.Store(true)
	close(hook.doneCh) // grace-cancel lands inside the check window
	select {
	case <-store.extendEntered:
		// Pre-fix order: detached mode was entered before the check, so
		// the cancel parked the loop inside a detached renewal.
	case <-time.After(300 * time.Millisecond):
		// Post-fix order: the flag is still clear, so the loop exited
		// without issuing.
	}

	// The preceding ReleaseLease: Shutdown's releaseInFlight removes and
	// releases the still-tracked task while the handler is frozen.
	w.releaseInFlight(context.Background())

	close(hook.errRelease) // the check reports live; the handler attempts the transfer
	select {
	case herr := <-herrCh:
		if herr != nil {
			close(store.extendGate)
			t.Fatalf("handleActivity = %v, want nil (stale result is dropped, not an error)", herr)
		}
	case <-time.After(5 * time.Second):
		close(store.extendGate)
		t.Fatal("handler did not return after the transfer failed")
	}
	close(store.extendGate)
	time.Sleep(200 * time.Millisecond) // let any gated renewal land

	// No result commit may run after ownership was lost: the transfer
	// failed, so CompleteActivity must never have run.
	if n := store.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls=%d, want 0 (commit must be skipped once the lease was released)", n)
	}
	// No renewal may complete after the preceding release.
	if a, b := store.indexOf("extend-exit"), store.indexOf("release-exit"); b < 0 {
		t.Fatalf("lease op order = %v, want the shutdown release to have landed", store.snapshot())
	} else if a >= 0 && a >= b {
		t.Fatalf("lease op order = %v, want no renewal after the release", store.snapshot())
	}
	// The release must stand: the task is claimable by a peer instead of
	// re-hidden by a late renewal.
	if peer := probeActivityTasks(t, ctx, mem); len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1 (late renewal must not re-hide the released task)", len(peer))
	}
}

// TestWorker_DetachedEntryHoldsSingleCriticalSection is a regression test
// for the early-commit ordering finding: the unregistered/registry-error
// paths must enter detached mode BEFORE transferring ownership out of the
// in-flight set, atomically under the same mutex. If the transfer lands
// first, a cancel/renewal-tick in the gap observes neither ownership nor
// detached mode, the loop exits, and the detached nack that follows runs
// without renewal past the lease (peer reclaim, then the task-ID-only
// nack modifies the peer's task).
//
// The test hammers beginDetachedCommit while snapshot observers (holding
// the same mutex, exactly like releaseInFlight's collection and the
// renewal loop's ownership check) assert the invariant: while a commit
// entry is outstanding, the entry is either still present or detached
// mode is already entered — never neither. The atomic
// check-enter-transfer holds this invariant by construction, so the test
// passes deterministically; the split transfer-then-set order exposes the
// gap to the observers and fails.
func TestWorker_DetachedEntryHoldsSingleCriticalSection(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	const id int64 = 99
	const iters = 20000

	var committing atomic.Bool
	var phase atomic.Int32 // 1 while a commit entry is outstanding
	var violations atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// One consistent snapshot under the in-flight mutex,
				// mirroring releaseInFlight's collection.
				w.mu.Lock()
				_, present := w.inFlight[id]
				flag := committing.Load()
				ph := phase.Load()
				w.mu.Unlock()
				if ph == 1 && !present && !flag {
					violations.Add(1)
					return
				}
			}
		}()
	}
	for i := 0; i < iters; i++ {
		tok := w.track(id)
		committing.Store(false)
		phase.Store(1)
		if !w.beginDetachedCommit(id, tok, &committing, context.Background()) {
			close(stop)
			wg.Wait()
			t.Fatalf("iter %d: beginDetachedCommit failed on an owned entry", i)
		}
		phase.Store(0)
		// Reset for the next iteration outside any outstanding entry.
		_ = w.track(id)
		committing.Store(false)
		w.mu.Lock()
		delete(w.inFlight, id)
		w.mu.Unlock()
	}
	close(stop)
	wg.Wait()
	if v := violations.Load(); v != 0 {
		t.Fatalf("observers saw %d states with neither ownership nor detached mode during commit entry (transfer must not precede detached-mode entry)", v)
	}
}

// TestWorker_ConservativeLeaseExpiryIsPinned is a unit test for the
// conservative-timestamp finding: trackAt/refreshLeaseAt measure the
// lease from the instant BEFORE the claim/renewal store call, since
// backends stamp the visible lease during the call itself.
func TestWorker_ConservativeLeaseExpiryIsPinned(t *testing.T) {
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	w := NewWorker(mem, WorkerOptions{
		LeaseDuration: 10 * time.Second,
		WorkerID:      "w1",
	})
	at := time.Now().UTC().Add(-time.Hour) // arbitrary base, even in the past
	tok := w.trackAt(5, at)
	defer w.untrack(5, tok)
	w.mu.Lock()
	expiry := w.inFlight[5].expiry
	w.mu.Unlock()
	if !expiry.Equal(at.Add(10 * time.Second)) {
		t.Fatalf("trackAt expiry=%v, want exactly base+lease (%v)", expiry, at.Add(10*time.Second))
	}

	renewBase := time.Now().UTC().Add(-time.Minute)
	w.refreshLeaseAt(5, tok, renewBase)
	w.mu.Lock()
	expiry = w.inFlight[5].expiry
	w.mu.Unlock()
	if !expiry.Equal(renewBase.Add(10 * time.Second)) {
		t.Fatalf("refreshLeaseAt expiry=%v, want exactly base+lease (%v)", expiry, renewBase.Add(10*time.Second))
	}

	// A stale token must not move the entry.
	stale := claimToken{epoch: tok.epoch, seq: tok.seq + 1000}
	w.refreshLeaseAt(5, stale, renewBase.Add(time.Hour))
	w.mu.Lock()
	expiry = w.inFlight[5].expiry
	w.mu.Unlock()
	if !expiry.Equal(renewBase.Add(10 * time.Second)) {
		t.Fatalf("stale refreshLeaseAt moved expiry to %v, want it unchanged", expiry)
	}
}

// TestWorker_ClaimTracksConservativeExpiry is a regression test for the
// conservative-timestamp finding on the claim path: the local lease
// must be measured from before ClaimTasks returns, not after. A backend
// that stamps the visible lease during the (slow) claim would otherwise
// leave a gap where the local estimate extends past the actual lease and
// a peer reclaim inside the gap still looks valid locally.
func TestWorker_ClaimTracksConservativeExpiry(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	// Claim latency the local estimate must not absorb: the expiry is
	// pinned to the pre-claim instant.
	const claimLatency = 300 * time.Millisecond
	gater := &slowClaimBackend{Backend: mem, delay: claimLatency}
	w := NewWorker(gater, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, gater, mem, w, "conservative-claim-1", "hooked")
	// The setup claim holds the task under the frozen store clock; move
	// past it so the test worker's own (slow) claim succeeds.
	mem.SetNow(t0.Add(time.Second))
	// Block inside the activity itself: the entry is transferred out of
	// the in-flight set at commit time, so the tracked expiry is only
	// observable while the activity runs.
	actEntered := make(chan struct{})
	actRelease := make(chan struct{})
	var actOnce sync.Once
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		actOnce.Do(func() { close(actEntered) })
		<-actRelease
		return "ok", nil
	}, WithName("hooked"))

	tickDone := make(chan struct{})
	go func() { w.tickActivitiesSync(ctx); close(tickDone) }()
	select {
	case <-actEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("activity did not start")
	}
	// The activity is running: the entry is still tracked, so its expiry
	// is observable. It must precede the post-claim now+lease by the claim
	// latency (with margin for scheduling jitter).
	w.mu.Lock()
	expiry, ok := w.inFlight[task.ID]
	w.mu.Unlock()
	if !ok {
		close(actRelease)
		<-tickDone
		t.Fatal("running task is not tracked")
	}
	threshold := time.Now().Add(10*time.Second - 150*time.Millisecond)
	if !expiry.expiry.Before(threshold) {
		close(actRelease)
		<-tickDone
		t.Fatalf("local expiry=%v, want before %v (pre-claim base, not post-claim)", expiry.expiry, threshold)
	}
	close(actRelease)
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("tick did not finish")
	}
}

// TestWorker_RenewRefreshesConservativeExpiry is a regression test for
// the conservative-timestamp finding on the renewal path: the refreshed
// local lease must be measured from before ExtendLease returns, not
// after, or the same peer-reclaim gap opens on every renewal.
func TestWorker_RenewRefreshesConservativeExpiry(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	// Renewal latency the local estimate must not absorb.
	const renewLatency = 200 * time.Millisecond
	store := &sleepExtendBackend{Backend: mem, delay: renewLatency}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration:          10 * time.Second,
		WorkerID:               "w1",
		IncompatibleRetryDelay: -1,
	})
	// A real backend row so ExtendLease succeeds and the refresh path
	// actually runs ("ghost" stays unregistered; the task itself is only
	// used for its claim and lease row).
	task := setupClaimableActivityTask(t, ctx, store, mem, w, "conservative-renew-1", "ghost")
	tok := w.trackTaskAt(task, time.Now())
	defer w.untrack(task.ID, tok)
	before := time.Now()
	w.renewOnceDetached(ctx, task.ID, tok)
	after := time.Now()
	w.mu.Lock()
	expiry, ok := w.inFlight[task.ID]
	w.mu.Unlock()
	if !ok {
		t.Fatal("renewed task is not tracked")
	}
	// Pinned to the pre-renewal base: well before post-call now+lease.
	threshold := after.Add(10*time.Second - 100*time.Millisecond)
	if !expiry.expiry.Before(threshold) {
		t.Fatalf("renewed expiry=%v, want before %v (pre-renewal base, not post-call)", expiry.expiry, threshold)
	}
	// ... but not earlier than the pre-call base (sanity: the renewal
	// still extended the lease forward).
	if expiry.expiry.Before(before.Add(10*time.Second - 50*time.Millisecond)) {
		t.Fatalf("renewed expiry=%v, want at/after pre-call base+lease", expiry.expiry)
	}
}
