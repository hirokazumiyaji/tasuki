package tasuki

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/observability"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// round27RequeueOrderBackend orders a failed workflow commit against a
// blocked periodic cover renewal: the first ExtendLease (initial cover)
// delegates so continuity is proven, later ones block on a
// test-controlled gate while IGNORING the passed context, then delegate
// so the late call LANDS. CommitAdvancement holds until the periodic
// renewal is blocked, then fails, so the failure requeue races the
// blocked renewal deterministically. NackTask records its invocation for
// the ordering assertion.
type round27RequeueOrderBackend struct {
	backend.Backend
	mu            sync.Mutex
	events        []string
	extendCalls   atomic.Int32
	extendEntered chan struct{}
	extendOnce    atomic.Bool
	extendRelease chan struct{}
	commitErrored chan struct{}
	commitOnce    atomic.Bool
	nackCalled    chan struct{}
	nackOnce      atomic.Bool
}

func (b *round27RequeueOrderBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	if b.extendCalls.Add(1) == 1 {
		return b.Backend.ExtendLease(ctx, taskID, d)
	}
	if b.extendOnce.CompareAndSwap(false, true) {
		close(b.extendEntered)
	}
	// Deliberately ignore ctx: block until the test releases, then land.
	<-b.extendRelease
	err := b.Backend.ExtendLease(context.Background(), taskID, d)
	b.mu.Lock()
	b.events = append(b.events, "extend-return")
	b.mu.Unlock()
	return err
}

func (b *round27RequeueOrderBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	// Hold the commit until the periodic cover renewal is blocked in
	// the backend, so the failure requeue races it deterministically.
	select {
	case <-b.extendEntered:
	case <-time.After(15 * time.Second):
		return errors.New("round27: periodic cover renewal never started")
	}
	if b.commitOnce.CompareAndSwap(false, true) {
		close(b.commitErrored)
	}
	return errors.New("round27: injected commit failure")
}

func (b *round27RequeueOrderBackend) NackTask(ctx context.Context, task backend.Task, delay time.Duration) error {
	b.mu.Lock()
	b.events = append(b.events, "nack")
	b.mu.Unlock()
	if b.nackOnce.CompareAndSwap(false, true) {
		close(b.nackCalled)
	}
	return b.Backend.NackTask(ctx, task, delay)
}

// TestWorker_Round27_FailedCommitRequeueJoinsCover is the regression test
// for round-27 P2a (join cover renewals before requeueing failed
// workflows). When CommitAdvancement errors while a periodic workflow
// cover ExtendLease is blocked in a context-ignoring backend, the
// failure requeue (ReleaseLease/NackTask) rewrites visible_at in place —
// a blocked renewal landing after it overwrites what it wrote, hiding a
// conflict retry for a full lease or replacing the nack's
// IncompatibleRetryDelay.
//
// Layout: one gated pending with proven initial cover; CommitAdvancement
// holds until the periodic renewal blocks, then fails. The fix stops new
// cover and joins the blocked renewal (bounded) before requeueing, so
// the nack runs strictly after the renewal lands. Without the fix the
// nack runs immediately while the renewal is still blocked.
func TestWorker_Round27_FailedCommitRequeueJoinsCover(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := &round27RequeueOrderBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
		commitErrored: make(chan struct{}),
		nackCalled:    make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 400 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = 400 * time.Millisecond
	claimStart := time.Now()
	task, _, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round27-requeue-1", lease, claimStart)
	pending := round23Pending(task, st, w.trackTaskAt(task, time.Now()))

	released := false
	defer func() {
		if !released {
			close(store.extendRelease)
		}
	}()

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(ctx, []pendingWorkflowCommit{pending})
	}()
	select {
	case <-store.commitErrored:
	case <-time.After(15 * time.Second):
		t.Fatal("commit did not fail while periodic cover was blocked")
	}
	// The failure requeue must wait for the blocked cover renewal: the
	// nack must not run while the renewal is still in flight. 300ms is
	// far above the microseconds the error path needs to reach the join.
	select {
	case <-store.nackCalled:
		released = true
		close(store.extendRelease)
		t.Fatal("nack ran while a cover renewal was still blocked (failure requeue must join cover first)")
	case <-time.After(300 * time.Millisecond):
	}
	released = true
	close(store.extendRelease)
	select {
	case <-flushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not return after cover settled")
	}
	select {
	case <-store.nackCalled:
	case <-time.After(15 * time.Second):
		t.Fatal("nack never ran after cover settled")
	}

	store.mu.Lock()
	events := append([]string(nil), store.events...)
	store.mu.Unlock()
	extendIdx, nackIdx := -1, -1
	for i, e := range events {
		switch e {
		case "extend-return":
			if extendIdx < 0 {
				extendIdx = i
			}
		case "nack":
			if nackIdx < 0 {
				nackIdx = i
			}
		}
	}
	if extendIdx < 0 || nackIdx < 0 {
		t.Fatalf("events = %v, want both extend-return and nack", events)
	}
	if nackIdx < extendIdx {
		t.Fatalf("events = %v, want extend-return before nack (requeue must land after the joined renewal, not before it)", events)
	}
}

// TestWorker_Round27_StaleRenewalCompensatedWhileFresh is the regression
// test for round-27 P2b (compensate landed-after-trip renewals even when
// the original lease still looks fresh). When CommitTimeout expires while
// the initial cover renewal is blocked in a context-ignoring backend,
// the bounded Phase 1 join trips the guard and the flush admits no
// commit — but the blocked ExtendLease may still land afterwards, here
// just BEFORE the original expiry. Suppressing the compensation on
// freshness leaves the unowned task hidden for a full lease; the
// token-fenced release is safe even pre-reclaim (no peer can hold the
// lease before the original expiry), so the fix always compensates.
//
// Layout (real store clock): 1s lease, 200ms commit bound, ExtendLease
// blocked ignoring ctx and released at ~500ms (original expiry ~1s). The
// fix issues the fenced release on landing, so a peer reclaims promptly
// at ~800ms. Without the fix the late landing hides the task until
// ~1.5s and the peer claim finds nothing.
func TestWorker_Round27_StaleRenewalCompensatedWhileFresh(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	store := &round26BlockExtendBackend{
		Backend:       mem,
		extendEntered: make(chan struct{}),
		extendRelease: make(chan struct{}),
	}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: time.Second,
		WorkerID:      "w1",
		Metrics:       m,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	const lease = time.Second
	claimStart := time.Now()
	task, _, st := round23SetupWorkflowClaim(t, ctx, mem, w, "round27-fresh-1", lease, claimStart)
	nextBefore := st.NextSeq
	pending := round23Pending(task, st, w.trackTaskAt(task, time.Now()))

	commitCtx, commitCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer commitCancel()
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		w.flushWorkflowCommits(commitCtx, []pendingWorkflowCommit{pending})
	}()
	select {
	case <-store.extendEntered:
	case <-time.After(15 * time.Second):
		t.Fatal("initial cover renewal did not start")
	}
	select {
	case <-flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not return within 5s while ExtendLease stalled ignoring ctx")
	}
	if n := store.commitCalls.Load(); n != 0 {
		t.Fatalf("CommitAdvancement calls = %d, want 0 (unproven continuity must skip the store op)", n)
	}

	// Let the stale renewal land well BEFORE the original expiry, then
	// give the synchronous compensation scheduling slack (far below the
	// full-lease hiding the late landing causes without the fix).
	if wait := time.Until(claimStart.Add(500 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	close(store.extendRelease)
	time.Sleep(300 * time.Millisecond)

	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatal("peer could not reclaim promptly after the stale renewal landed before expiry (want a fenced compensation release even on a fresh lease)")
	}
	stAfter, err := mem.LoadWorkflow(ctx, "round27-fresh-1")
	if err != nil {
		t.Fatal(err)
	}
	if stAfter.NextSeq != nextBefore {
		t.Fatalf("head nextSeq = %d, want %d (stalled cover must not apply)", stAfter.NextSeq, nextBefore)
	}
	if n := storeErrorTotal(t, ctx, reader, "commit_workflow"); n != 0 {
		t.Fatalf("store_errors{op=commit_workflow} = %d, want 0 (skipped fenced commit is not a store failure)", n)
	}
	if n := storeErrorTotal(t, ctx, reader, "extend_lease"); n != 0 {
		t.Fatalf("store_errors{op=extend_lease} = %d, want 0 (dropped stale renewal is fencing, not a store failure)", n)
	}
}

// counterTotal sums one OTEL counter instrument by name.
func counterTotal(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("metric collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s data is %T, want Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

// TestWorker_Round27_RejectedRetryCountsNoMetric is the regression test
// for round-27 P2c (count activity retries only after scheduling
// succeeds). The detached gate can reject the retry commit
// (continuity/join-timeout) with errLeaseLost without running
// RetryActivity — counting before the gate then records a phantom retry
// that never ran.
//
// Layout: failing activity (retry path), 30s lease (no ticker renewal),
// planted ordinary renewal holding the pre-write join, guard tripped
// mid-join (same deterministic fencing as the round-22 store-error
// test). The fix moves the counter past the gate, so the rejection
// counts zero retries while issuing no store op. Without the fix the
// counter reads 1 despite no retry being scheduled.
func TestWorker_Round27_RejectedRetryCountsNoMetric(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round27-nometric-1", "hooked")

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	store := &countResultBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 30 * time.Second, // no ticker renewal during the test
		WorkerID:      "w1",
		Metrics:       m,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "", errors.New("boom")
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	// Hold the exclusive-commit ordinary-renewal join open: the handler
	// cannot reach its re-gate (and return) before this is released.
	if !w.renewTryEnter(task.ID, tok) {
		t.Fatal("renewTryEnter = false, want true (fresh worker admits the plant)")
	}
	plantRelease := make(chan struct{})
	plantDone := make(chan struct{})
	go func() {
		defer close(plantDone)
		<-plantRelease
		w.renewExit(task.ID)
	}()
	defer func() {
		select {
		case <-plantRelease:
		default:
			close(plantRelease)
		}
		<-plantDone
	}()

	herrCh := make(chan error, 1)
	go func() { herrCh <- w.handleActivity(ctx, task, tok) }()

	// The activity fails instantly and the synchronous pre-commit
	// renewal (memory backend) settles in microseconds; 100ms puts the
	// handler deterministically inside the held join before the trip, so
	// the rejection comes from the commit gate (past the counter site),
	// not from a missing renewal.
	time.Sleep(100 * time.Millisecond)
	tripStop := make(chan struct{})
	tripDone := make(chan struct{})
	go func() {
		defer close(tripDone)
		for {
			select {
			case <-tripStop:
				return
			default:
			}
			w.tripDetachedGuard(task.ID, tok)
		}
	}()
	// The tight trip surely drops the guard while the join is held, so
	// the re-gate after the release finds the lease lost.
	time.Sleep(100 * time.Millisecond)
	close(plantRelease)
	<-plantDone
	close(tripStop)
	<-tripDone

	select {
	case herr := <-herrCh:
		if !errors.Is(herr, errLeaseLost) {
			t.Fatalf("handleActivity = %v, want errLeaseLost (tripped guard must abort the retry commit)", herr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("handleActivity did not return after the plant released")
	}
	if n := store.retryCalls.Load(); n != 0 {
		t.Fatalf("RetryActivity calls = %d, want 0 (fencing rejection must issue no store op)", n)
	}
	if n := store.extends.Load(); n == 0 {
		t.Fatal("ExtendLease calls = 0, want >= 1 (the pre-commit renewal must run; the rejection must come from the commit gate, not a missing renewal)")
	}
	if n := counterTotal(t, ctx, reader, "tasuki.activity.retries"); n != 0 {
		t.Fatalf("tasuki.activity.retries = %d, want 0 (a gate-rejected retry schedules nothing and must count nothing)", n)
	}
}

// TestWorker_Round27_SuccessfulRetryCountsOne guards the other side of
// the round-27 P2c move: a retry the store accepts still counts exactly
// once.
func TestWorker_Round27_SuccessfulRetryCountsOne(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round27-counted-1", "hooked")

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	store := &countResultBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 30 * time.Second,
		WorkerID:      "w1",
		Metrics:       m,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		return "", errors.New("boom")
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	if herr := w.handleActivity(ctx, task, tok); herr != nil {
		t.Fatalf("handleActivity = %v, want nil (accepted retry)", herr)
	}
	if n := store.retryCalls.Load(); n != 1 {
		t.Fatalf("RetryActivity calls = %d, want 1", n)
	}
	if n := counterTotal(t, ctx, reader, "tasuki.activity.retries"); n != 1 {
		t.Fatalf("tasuki.activity.retries = %d, want 1 (an accepted retry must still count exactly once)", n)
	}
}
