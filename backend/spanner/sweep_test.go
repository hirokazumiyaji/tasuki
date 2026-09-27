package spanner

import (
	"fmt"
	"testing"
	"time"
)

func TestSweepBatchSizeBounded(t *testing.T) {
	// Well under Spanner's ~20k mutation cap: even a full page plus the
	// instance/status mutations of a terminal commit stays tiny.
	if spannerSweepBatchSize > 5000 {
		t.Fatalf("sweep batch %d too large for safe commits", spannerSweepBatchSize)
	}
}

func TestSignalDedupeSweepTimeoutBounded(t *testing.T) {
	// The post-commit dedupe sweep must stay synchronous (a redelivered
	// DedupeID inserts anew once CommitAdvancements returns) yet bounded, so
	// a degraded store delays only cleanup while terminal notification has
	// already fired.
	if signalDedupeSweepTimeout <= 0 || signalDedupeSweepTimeout > 5*time.Minute {
		t.Fatalf("signal dedupe sweep timeout %v must be a positive bound", signalDedupeSweepTimeout)
	}
}

func TestBatchesNeededChunking(t *testing.T) {
	cases := []struct {
		total, size, want int
	}{
		{0, spannerSweepBatchSize, 0},
		{1, spannerSweepBatchSize, 1},
		{spannerSweepBatchSize, spannerSweepBatchSize, 1},
		{spannerSweepBatchSize + 1, spannerSweepBatchSize, 2},
		// 600 dedupe rows (the regression case for issue #296) must take
		// more than one commit.
		{600, spannerSweepBatchSize, 2},
		{600, 500, 2},
		{2500, 500, 5},
	}
	for _, c := range cases {
		if got := batchesNeeded(c.total, c.size); got != c.want {
			t.Fatalf("batchesNeeded(%d,%d)=%d want %d", c.total, c.size, got, c.want)
		}
	}
}

func TestChunkHelpersPaging(t *testing.T) {
	var strs []string
	for i := 0; i < 600; i++ {
		strs = append(strs, fmt.Sprintf("dedupe-%04d", i))
	}
	sc := chunkStrings(strs, spannerSweepBatchSize)
	if len(sc) != 2 || len(sc[0]) != spannerSweepBatchSize || len(sc[1]) != 600-spannerSweepBatchSize {
		t.Fatalf("600 string keys must split %d+%d, got %v", spannerSweepBatchSize, 600-spannerSweepBatchSize, lensStr(sc))
	}
	seen := map[string]bool{}
	for _, c := range sc {
		for _, k := range c {
			if seen[k] {
				t.Fatalf("duplicate key %q after chunking", k)
			}
			seen[k] = true
		}
	}
	if len(seen) != 600 {
		t.Fatalf("lost keys in chunking: got %d want 600", len(seen))
	}

	var nums []int64
	for i := int64(1); i <= 600; i++ {
		nums = append(nums, i)
	}
	nc := chunkInt64s(nums, spannerSweepBatchSize)
	if len(nc) != 2 || len(nc[0]) != spannerSweepBatchSize || len(nc[1]) != 600-spannerSweepBatchSize {
		t.Fatalf("600 int keys must split %d+%d, got %v", spannerSweepBatchSize, 600-spannerSweepBatchSize, lensInt(nc))
	}
	if nc[0][0] != 1 || nc[1][0] != int64(spannerSweepBatchSize+1) {
		t.Fatalf("chunk order not preserved: %d %d", nc[0][0], nc[1][0])
	}
	if got := chunkStrings(nil, 10); len(got) != 0 {
		t.Fatalf("nil input must yield no chunks, got %v", got)
	}
	if got := chunkInt64s(nil, 10); len(got) != 0 {
		t.Fatalf("nil input must yield no chunks, got %v", got)
	}
}

func lensStr(chunks [][]string) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}

func lensInt(chunks [][]int64) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}
