package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// Config controls a benchmark run. Zero values use defaults.
type Config struct {
	Workers    int
	Instances  int
	Steps      int
	Poll       time.Duration
	Duration   time.Duration
	Lease      time.Duration
	ClaimLimit int // 0 = Worker default (10)
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
	return c
}

// Run starts in-process workers, starts Instances workflows, waits for completion
// (or Duration), and returns throughput metrics.
func Run(ctx context.Context, b backend.Backend, backendName string, cfg Config) (Result, error) {
	cfg = cfg.withDefaults()
	workers := make([]*tasuki.Worker, 0, cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := tasuki.NewWorker(b, tasuki.WorkerOptions{
			PollInterval:  cfg.Poll,
			LeaseDuration: cfg.Lease,
			ClaimLimit:    cfg.ClaimLimit,
			WorkerID:      fmt.Sprintf("bench-w-%d", i),
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		Register(w)
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
	handles := make([]*tasuki.Handle, 0, cfg.Instances)
	for i := 0; i < cfg.Instances; i++ {
		id := fmt.Sprintf("bench-%d", i)
		h, err := tasuki.Start(ctx, client, WorkflowName, cfg.Steps, tasuki.WithID(id))
		if err != nil {
			return Result{}, fmt.Errorf("start %s: %w", id, err)
		}
		handles = append(handles, h)
	}

	waitCtx := ctx
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	start := time.Now()
	var completed, failed atomic.Int64
	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(h *tasuki.Handle) {
			defer wg.Done()
			_, err := tasuki.Result[int](waitCtx, h)
			if err != nil {
				// Duration cut-off / cancel: incomplete, not failed.
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || waitCtx.Err() != nil {
					return
				}
				failed.Add(1)
				return
			}
			completed.Add(1)
		}(h)
	}
	wg.Wait()
	wall := time.Since(start).Seconds()
	comp := int(completed.Load())
	fail := int(failed.Load())
	throughput := 0.0
	if wall > 0 {
		throughput = float64(comp) / wall
	}
	res := Result{
		Backend:     backendName,
		Workers:     cfg.Workers,
		Instances:   cfg.Instances,
		Steps:       cfg.Steps,
		Completed:   comp,
		Failed:      fail,
		WallSeconds: wall,
		Throughput:  throughput,
	}
	if cfg.Duration == 0 && fail > 0 {
		return res, fmt.Errorf("bench: %d failed", fail)
	}
	return res, nil
}
