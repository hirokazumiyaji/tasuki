package tasuki

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/observability"
)

// countingBackend wraps a Backend and counts CountClaimableTasks calls.
// Each backlog sample issues one call per kind (workflow + activity = 2).
type countingBackend struct {
	backend.Backend
	calls atomic.Int64
}

func (b *countingBackend) CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error) {
	b.calls.Add(1)
	return b.Backend.CountClaimableTasks(ctx, kind, queues)
}

func TestBacklogSampleInterval_Default(t *testing.T) {
	opts := WorkerOptions{}.withDefaults()
	if opts.BacklogSampleInterval != 10*time.Second {
		t.Fatalf("default BacklogSampleInterval = %v, want 10s", opts.BacklogSampleInterval)
	}
	if opts.BacklogSampleInterval <= opts.PollInterval {
		t.Fatalf("default BacklogSampleInterval (%v) must be coarser than PollInterval (%v)",
			opts.BacklogSampleInterval, opts.PollInterval)
	}
	// Zero value wires through NewWorker.
	w := NewWorker(memory.New(), WorkerOptions{
		Queues:  []string{"default"},
		Metrics: observability.MustNewMetrics(),
	})
	if w.opts.BacklogSampleInterval != 10*time.Second {
		t.Fatalf("NewWorker BacklogSampleInterval = %v, want 10s", w.opts.BacklogSampleInterval)
	}
}

func TestSampleBacklog_ThrottlesAcrossTicks(t *testing.T) {
	ctx := context.Background()
	b := &countingBackend{Backend: memory.New()}
	w := NewWorker(b, WorkerOptions{
		Queues:  []string{"default"},
		Metrics: observability.MustNewMetrics(),
		// Default 10s: rapid ticks must not re-sample.
	})

	w.tick(ctx)
	if got := b.calls.Load(); got != 2 {
		t.Fatalf("after first tick CountClaimableTasks calls = %d, want 2 (workflow+activity)", got)
	}
	// Second immediate tick (including NOTIFY-wake path) must not re-sample.
	w.tick(ctx)
	if got := b.calls.Load(); got != 2 {
		t.Fatalf("after second tick calls = %d, want still 2 (throttled)", got)
	}
	// PollOnce path shares the same throttle.
	w.tickSync(ctx)
	if got := b.calls.Load(); got != 2 {
		t.Fatalf("after tickSync calls = %d, want still 2 (throttled)", got)
	}
}

func TestSampleBacklog_ConfigurableInterval(t *testing.T) {
	ctx := context.Background()
	b := &countingBackend{Backend: memory.New()}
	w := NewWorker(b, WorkerOptions{
		Queues:                []string{"default"},
		Metrics:               observability.MustNewMetrics(),
		BacklogSampleInterval: 50 * time.Millisecond,
	})
	if w.opts.BacklogSampleInterval != 50*time.Millisecond {
		t.Fatalf("custom interval not wired: got %v", w.opts.BacklogSampleInterval)
	}

	w.sampleBacklog(ctx)
	if got := b.calls.Load(); got != 2 {
		t.Fatalf("first sample calls = %d, want 2", got)
	}
	w.sampleBacklog(ctx)
	if got := b.calls.Load(); got != 2 {
		t.Fatalf("immediate re-sample calls = %d, want still 2", got)
	}
	time.Sleep(60 * time.Millisecond)
	w.sampleBacklog(ctx)
	if got := b.calls.Load(); got != 4 {
		t.Fatalf("after interval expiry calls = %d, want 4", got)
	}
}

func TestSampleBacklog_DisabledWhenNegative(t *testing.T) {
	ctx := context.Background()
	b := &countingBackend{Backend: memory.New()}
	w := NewWorker(b, WorkerOptions{
		Queues:                []string{"default"},
		Metrics:               observability.MustNewMetrics(),
		BacklogSampleInterval: -1,
	})
	w.sampleBacklog(ctx)
	w.tick(ctx)
	if got := b.calls.Load(); got != 0 {
		t.Fatalf("disabled sampling calls = %d, want 0", got)
	}
}

func TestSampleBacklog_NilMetricsSkipsStore(t *testing.T) {
	ctx := context.Background()
	b := &countingBackend{Backend: memory.New()}
	w := NewWorker(b, WorkerOptions{Queues: []string{"default"}})
	w.sampleBacklog(ctx)
	w.tick(ctx)
	if got := b.calls.Load(); got != 0 {
		t.Fatalf("nil Metrics calls = %d, want 0", got)
	}
}
