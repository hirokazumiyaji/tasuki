package bench_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/bench"
)

func TestRun_MemoryThroughput(t *testing.T) {
	b := memory.New()
	res, err := bench.Run(context.Background(), b, "memory", bench.Config{
		Workers:   2,
		Instances: 20,
		Steps:     3,
		Poll:      10 * time.Millisecond,
		Lease:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Completed != 20 || res.Failed != 0 {
		t.Fatalf("got completed=%d failed=%d", res.Completed, res.Failed)
	}
	if res.Throughput <= 0 || res.WallSeconds <= 0 {
		t.Fatalf("bad metrics: %+v", res)
	}
}
