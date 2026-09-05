package bench_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/bench"
)

func TestRun_E2EIncludesSubmitAndLatency(t *testing.T) {
	b := memory.New()
	res, err := bench.Run(context.Background(), b, "memory", bench.Config{
		Workers:   2,
		Instances: 10,
		Steps:     2,
		Poll:      5 * time.Millisecond,
		Lease:     time.Second,
		RunID:     "e2e-test",
		Scenario:  "chain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Completed != 10 {
		t.Fatalf("completed=%d", res.Completed)
	}
	if res.RunID != "e2e-test" || res.Scenario != "chain" {
		t.Fatalf("%+v", res)
	}
	if res.LatencyP50 <= 0 || res.LatencyP95 <= 0 || res.LatencyP99 <= 0 {
		t.Fatalf("latency missing: %+v", res)
	}
	if res.LatencyP50 > res.LatencyP95 || res.LatencyP95 > res.LatencyP99 {
		t.Fatalf("latency order: %+v", res)
	}
	if res.Settings == nil || res.Settings["run_id"] != "e2e-test" {
		t.Fatalf("settings missing run_id: %+v", res.Settings)
	}
}

func TestRun_RunIsolationAndRepeat(t *testing.T) {
	b := memory.New()
	for _, run := range []string{"rep-a", "rep-b"} {
		res, err := bench.Run(context.Background(), b, "memory", bench.Config{
			Workers:   2,
			Instances: 5,
			Steps:     1,
			Poll:      5 * time.Millisecond,
			Lease:     time.Second,
			RunID:     run,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Completed != 5 {
			t.Fatalf("run %s completed=%d", run, res.Completed)
		}
	}
	// Existing data preserved: both runs' instances exist (10 total).
	list, err := b.ListInstances(context.Background(), backend.InstanceFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 10 {
		t.Fatalf("expected 10 preserved instances, got %d", len(list))
	}
}

func TestRun_Scenarios(t *testing.T) {
	for _, sc := range []string{"chain", "mixed", "long-history"} {
		b := memory.New()
		res, err := bench.Run(context.Background(), b, "memory", bench.Config{
			Workers:   2,
			Instances: 5,
			Steps:     3,
			Poll:      5 * time.Millisecond,
			Lease:     time.Second,
			Scenario:  sc,
			RunID:     "sc-" + sc,
		})
		if err != nil {
			t.Fatalf("%s: %v", sc, err)
		}
		if res.Completed != 5 {
			t.Fatalf("%s completed=%d", sc, res.Completed)
		}
	}
}
