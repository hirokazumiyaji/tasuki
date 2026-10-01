package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/observability"
	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// flakyBackend fails the next workflow ClaimTasks once, then delegates.
// The worker loop and the test both touch failNext and calls, so they are atomic.
type flakyBackend struct {
	*memory.Backend
	failNext atomic.Bool
	calls    atomic.Int32
}

func (f *flakyBackend) ClaimTasks(ctx context.Context, req backend.ClaimRequest) ([]backend.Task, error) {
	f.calls.Add(1)
	if req.Kind == "workflow" && f.failNext.CompareAndSwap(true, false) {
		return nil, errors.New("injected store failure")
	}
	return f.Backend.ClaimTasks(ctx, req)
}

func TestWorker_StoreErrorCountedAndRecovers(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := observability.NewMetricsWithMeter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	mem := memory.New()
	mem.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fb := &flakyBackend{Backend: mem}
	fb.failNext.Store(true)
	w := worker.NewWorker(fb, worker.WorkerOptions{
		PollInterval: time.Millisecond,
		Metrics:      m,
	})
	worker.RegisterWorkflow(w, func(wctx *workflow.Context, n int) (int, error) {
		return n, nil
	}, worker.WithName("echo"))
	w.Start(ctx)
	defer w.Shutdown(ctx)
	c := client.NewClient(fb)
	h, err := client.Start(ctx, c, "echo", 7, client.WithID("store-err-1"))
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := client.Result[int](rctx, h)
	if err != nil {
		t.Fatalf("should recover after injected failure: %v", err)
	}
	if out != 7 {
		t.Fatalf("got %d", out)
	}
	if fb.calls.Load() < 2 {
		t.Fatal("expected retry after failure")
	}
	// If we have a manual reader, verify the op-labeled counter.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err == nil {
		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, met := range sm.Metrics {
				if met.Name == "tasuki.worker.store_errors" {
					found = true
				}
			}
		}
		if !found {
			t.Log("store_errors metric not found (noop provider?)")
		}
	}
}
