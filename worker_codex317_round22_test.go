package tasuki

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/observability"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// countResultBackend counts detached result-commit store ops so tests can
// assert a fencing rejection issued nothing.
type countResultBackend struct {
	backend.Backend
	retryCalls    atomic.Int32
	completeCalls atomic.Int32
	extends       atomic.Int32
}

func (b *countResultBackend) ExtendLease(ctx context.Context, taskID int64, d time.Duration) error {
	err := b.Backend.ExtendLease(ctx, taskID, d)
	b.extends.Add(1)
	return err
}

func (b *countResultBackend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	b.retryCalls.Add(1)
	return b.Backend.RetryActivity(ctx, taskID, delay)
}

func (b *countResultBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	b.completeCalls.Add(1)
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

// storeErrorTotal sums tasuki.worker.store_errors, optionally filtered by op
// (empty op sums every op label).
func storeErrorTotal(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, op string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("metric collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "tasuki.worker.store_errors" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("store_errors data is %T, want Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				if op == "" {
					total += dp.Value
					continue
				}
				if v, ok := dp.Attributes.Value("op"); ok && v.AsString() == op {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// TestWorker_Round22_RetryLeaseLossSkipsStoreError is the regression test
// for round-22 P2 (lease-loss fencing must not count as a store failure).
// guardedDetachedCommit rejects the retry commit via the ordinary-renewal
// join re-gate — a planted renewal holds the join open while a concurrent
// trip drops the guard, so the gate finds the lease lost without running
// RetryActivity — and the retry branch must return errLeaseLost WITHOUT
// calling recordStoreError. Without the fix the branch records a
// retry_activity store failure (metric + warn log) for routine fencing,
// raising false backend-error alerts.
//
// Layout: failing activity (retry path), 30s lease (no ticker renewal),
// planted ordinary renewal holding the pre-write join, guard tripped
// mid-join. Deterministic: the handler cannot leave the join before the
// plant is released, and the trip surely lands during the hold.
func TestWorker_Round22_RetryLeaseLossSkipsStoreError(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round22-skip-1", "hooked")

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
	// handler deterministically inside the held join before the trip.
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
	if n := storeErrorTotal(t, ctx, reader, "retry_activity"); n != 0 {
		t.Fatalf("store_errors{op=retry_activity} = %d, want 0 (lease-loss fencing is not a store failure)", n)
	}
	if n := storeErrorTotal(t, ctx, reader, ""); n != 0 {
		t.Fatalf("total store_errors = %d, want 0 (no store op failed on the fencing path)", n)
	}
}

// TestWorker_Round22_CompleteHappyPathNoStoreError guards the other edited
// branch: a successful activity still commits via CompleteActivity exactly
// once with no store error recorded.
func TestWorker_Round22_CompleteHappyPathNoStoreError(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	setupW := NewWorker(mem, WorkerOptions{
		LeaseDuration:          200 * time.Millisecond,
		WorkerID:               "setup",
		IncompatibleRetryDelay: -1,
	})
	task := setupClaimableActivityTask(t, ctx, mem, mem, setupW, "round22-happy-1", "hooked")

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
		return "ok", nil
	}, WithName("hooked"))

	tok := w.track(task.ID)
	defer w.untrack(task.ID, tok)
	defer w.dropDetachedGuard(task.ID, tok)

	if herr := w.handleActivity(ctx, task, tok); herr != nil {
		t.Fatalf("handleActivity = %v, want nil", herr)
	}
	if n := store.completeCalls.Load(); n != 1 {
		t.Fatalf("CompleteActivity calls = %d, want 1", n)
	}
	if n := storeErrorTotal(t, ctx, reader, ""); n != 0 {
		t.Fatalf("total store_errors = %d, want 0 (clean completion records nothing)", n)
	}
}
