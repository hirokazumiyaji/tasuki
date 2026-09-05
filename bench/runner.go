package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// Config controls a benchmark run. Zero values use defaults.
type Config struct {
	Workers             int
	Instances           int
	Steps               int
	Poll                time.Duration
	Duration            time.Duration
	Lease               time.Duration
	ClaimLimit          int // 0 = Worker default (10)
	ActivityConcurrency int // 0 = Worker default (1)
	WorkflowConcurrency int // 0 = Worker default (1)
	// RunID isolates instance IDs for this run (default auto-generated).
	// Reusing the same store across runs is safe: IDs never collide and
	// existing data is preserved.
	RunID string
	// Scenario selects the representative load: "chain" (default serial
	// steps), "long-history" (many steps to stress replay), "mixed"
	// (short/long activity mix).
	Scenario string
}

func (c Config) withDefaults() Config {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.Instances <= 0 {
		c.Instances = 200
	}
	if c.Steps <= 0 {
		c.Steps = 3
	}
	if c.Poll <= 0 {
		c.Poll = 20 * time.Millisecond
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.RunID == "" {
		c.RunID = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	if c.Scenario == "" {
		c.Scenario = "chain"
	}
	return c
}

// effectiveSettings snapshots the resolved worker settings for Result.
func (c Config) effectiveSettings() map[string]any {
	return map[string]any{
		"poll_ms":              c.Poll.Milliseconds(),
		"lease_ms":             c.Lease.Milliseconds(),
		"claim_limit":          c.ClaimLimit,
		"activity_concurrency": c.ActivityConcurrency,
		"workflow_concurrency": c.WorkflowConcurrency,
		"scenario":             c.Scenario,
		"run_id":               c.RunID,
	}
}

// Run starts in-process workers, starts Instances workflows, waits for completion
// (or Duration), and returns throughput metrics.
//
// Measurement covers submit→completion end-to-end: the clock starts before the
// first Start and stops after the last Result, so instances that finish during
// submission are correctly included in both numerator and denominator.
// Per-instance latencies yield p50/p95/p99 in Result.
func Run(ctx context.Context, b backend.Backend, backendName string, cfg Config) (Result, error) {
	cfg = cfg.withDefaults()
	workers := make([]*tasuki.Worker, 0, cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := tasuki.NewWorker(b, tasuki.WorkerOptions{
			PollInterval:        cfg.Poll,
			LeaseDuration:       cfg.Lease,
			ClaimLimit:          cfg.ClaimLimit,
			ActivityConcurrency: cfg.ActivityConcurrency,
			WorkflowConcurrency: cfg.WorkflowConcurrency,
			WorkerID:            fmt.Sprintf("bench-w-%d", i),
			Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		RegisterScenario(w, cfg.Scenario)
		w.Start(ctx)
		workers = append(workers, w)
	}
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, w := range workers {
			_ = w.Shutdown(shCtx)
		}
	}()

	client := tasuki.NewClient(b)
	// E2E clock includes submission.
	e2eStart := time.Now()
	type submitted struct {
		h   *tasuki.Handle
		t0  time.Time
		idx int
	}
	submittedHandles := make([]submitted, 0, cfg.Instances)
	for i := 0; i < cfg.Instances; i++ {
		id := fmt.Sprintf("%s-%d", cfg.RunID, i)
		t0 := time.Now()
		h, err := tasuki.Start(ctx, client, WorkflowNameFor(cfg.Scenario), scenarioSteps(cfg), tasuki.WithID(id))
		if err != nil {
			return Result{}, fmt.Errorf("start %s: %w", id, err)
		}
		submittedHandles = append(submittedHandles, submitted{h: h, t0: t0, idx: i})
	}

	waitCtx := ctx
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	var completed, failed atomic.Int64
	latencies := make([]float64, cfg.Instances)
	var latMu sync.Mutex
	var wg sync.WaitGroup
	for _, s := range submittedHandles {
		wg.Add(1)
		go func(s submitted) {
			defer wg.Done()
			_, err := tasuki.Result[int](waitCtx, s.h)
			if err != nil {
				// Duration cut-off / cancel: incomplete, not failed.
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || waitCtx.Err() != nil {
					return
				}
				failed.Add(1)
				return
			}
			completed.Add(1)
			latMu.Lock()
			latencies[s.idx] = time.Since(s.t0).Seconds() * 1000
			latMu.Unlock()
		}(s)
	}
	wg.Wait()
	wall := time.Since(e2eStart).Seconds()
	comp := int(completed.Load())
	fail := int(failed.Load())
	throughput := 0.0
	if wall > 0 {
		throughput = float64(comp) / wall
	}
	// Latency distribution over completed instances only.
	var lats []float64
	for i := 0; i < comp; i++ {
		// latencies indexed by submit order; completed may be sparse when
		// Duration cuts off. Collect non-zero entries.
	}
	// Re-collect: iterate all, keep >0 (completed).
	for _, ms := range latencies {
		if ms > 0 {
			lats = append(lats, ms)
		}
	}
	p50, p95, p99 := percentiles(lats)
	res := Result{
		Backend:     backendName,
		Workers:     cfg.Workers,
		Instances:   cfg.Instances,
		Steps:       cfg.Steps,
		Scenario:    cfg.Scenario,
		RunID:       cfg.RunID,
		Completed:   comp,
		Failed:      fail,
		WallSeconds: wall,
		Throughput:  throughput,
		LatencyP50:  p50,
		LatencyP95:  p95,
		LatencyP99:  p99,
		Settings:    cfg.effectiveSettings(),
	}
	if cfg.Duration == 0 && fail > 0 {
		return res, fmt.Errorf("bench: %d failed", fail)
	}
	return res, nil
}

// percentiles returns p50/p95/p99 of ms values (0 when empty).
func percentiles(ms []float64) (p50, p95, p99 float64) {
	if len(ms) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(ms)
	at := func(q float64) float64 {
		idx := int(q * float64(len(ms)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(ms) {
			idx = len(ms) - 1
		}
		return ms[idx]
	}
	return at(0.50), at(0.95), at(0.99)
}
