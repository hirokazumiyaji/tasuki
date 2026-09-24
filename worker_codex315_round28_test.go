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
)

// round28LossExtendBackend fails every renewal with ErrNotFound,
// simulating a peer reclaim: the lease moved on before the first tick.
// Complete/Retry calls are counted so the test can prove the stale
// worker issues no terminal store op after the loss.
type round28LossExtendBackend struct {
	backend.Backend
	extends   atomic.Int32
	completes atomic.Int32
	retries   atomic.Int32
}

func (b *round28LossExtendBackend) ExtendLease(_ context.Context, _ backend.Task, _ time.Duration) error {
	b.extends.Add(1)
	return backend.ErrNotFound
}

func (b *round28LossExtendBackend) CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error {
	b.completes.Add(1)
	return b.Backend.CompleteActivity(ctx, taskID, ev)
}

func (b *round28LossExtendBackend) RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error {
	b.retries.Add(1)
	return b.Backend.RetryActivity(ctx, taskID, delay)
}

// TestWorker_Round28_ActivityCanceledOnLeaseLoss is the regression test
// for round-28 P1a (cancel activities when their lease is lost). A
// regular activity renewal that reports ErrNotFound (or proves
// unrestorable) used to stop the loop quietly while the activity kept
// running: the peer reclaimed and executed the same activity, and the
// stale worker then issued an ID-only Complete/Retry that deleted or
// rescheduled the peer's task with duplicate side effects.
//
// Layout: 200ms lease (first renewal tick at ~100ms fails with
// ErrNotFound), activity that observes its context and returns on cancel.
// The activity must observe cancellation promptly and the handler must
// issue no Complete/Retry. Without the fix the renewal stops quietly,
// the activity never observes a cancel (3s timer), and the stale
// Complete lands.
func TestWorker_Round28_ActivityCanceledOnLeaseLoss(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := &round28LossExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	var canceled atomic.Bool
	RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
			return "ok", nil
		}
	}, WithName("hooked"))

	task := backend.Task{
		ID:         999,
		Kind:       "activity",
		InstanceID: "round28-loss-1",
		Name:       "hooked",
		Attempt:    1,
		WorkerID:   "w1",
	}
	start := time.Now()
	herr := w.handleActivity(ctx, task, time.Now())
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("handleActivity elapsed = %v, want < 2s (lease loss at ~100ms must cancel the activity; without the fix the activity runs its 3s timer)", elapsed)
	}
	if !canceled.Load() {
		t.Fatal("activity did not observe cancellation (lease loss must cancel actCtx; without the fix the nil loss callback leaves it running)")
	}
	if n := store.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls = %d, want 0 (lease-lost result must not reach the ID-only commit)", n)
	}
	if n := store.retries.Load(); n != 0 {
		t.Fatalf("RetryActivity calls = %d, want 0 (lease-lost error must not reach the ID-only retry)", n)
	}
	if n := store.extends.Load(); n == 0 {
		t.Fatal("ExtendLease calls = 0, want >= 1 (the loss must come from a failed renewal, not a missing loop)")
	}
	if herr == nil {
		t.Fatal("handleActivity = nil, want lease-loss abandonment (result must not be reported as success)")
	}
}

// TestWorker_Round28_LeaseLostSkipsCancelIgnoringResult covers the
// cancel-ignoring activity: even when the activity function ignores the
// loss cancellation and returns a value, the post-return lease flag must
// still suppress the ID-only Complete.
func TestWorker_Round28_LeaseLostSkipsCancelIgnoringResult(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := &round28LossExtendBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		LeaseDuration: 200 * time.Millisecond,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	RegisterActivity(w, func(_ context.Context, _ struct{}) (string, error) {
		// Ignore cancellation deliberately: sleep past the first
		// renewal tick, then return success.
		time.Sleep(500 * time.Millisecond)
		return "ok", nil
	}, WithName("stubborn"))

	task := backend.Task{
		ID:         1000,
		Kind:       "activity",
		InstanceID: "round28-loss-2",
		Name:       "stubborn",
		Attempt:    1,
		WorkerID:   "w1",
	}
	herr := w.handleActivity(ctx, task, time.Now())
	if herr == nil {
		t.Fatal("handleActivity = nil, want lease-loss abandonment (cancel-ignoring success must still be suppressed)")
	}
	if n := store.completes.Load(); n != 0 {
		t.Fatalf("CompleteActivity calls = %d, want 0 (post-return flag check must suppress the stale write)", n)
	}
	if !errors.Is(herr, context.Canceled) && !errors.Is(herr, context.DeadlineExceeded) {
		t.Logf("handleActivity = %v (lease-loss abandonment; ctx-derived error)", herr)
	}
}
