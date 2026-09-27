package firestore

import (
	"fmt"
	"testing"
	"time"
)

func TestSweepBatchSizeBelowTxLimit(t *testing.T) {
	if firestoreSweepBatchSize >= firestoreTxWriteLimit {
		t.Fatalf("sweep batch %d must stay below tx limit %d", firestoreSweepBatchSize, firestoreTxWriteLimit)
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
		{0, firestoreSweepBatchSize, 0},
		{1, firestoreSweepBatchSize, 1},
		{firestoreSweepBatchSize, firestoreSweepBatchSize, 1},
		{firestoreSweepBatchSize + 1, firestoreSweepBatchSize, 2},
		// 600 dedupe rows (the regression case for issue #296) must take
		// more than one commit.
		{600, firestoreSweepBatchSize, 2},
		{600, 500, 2},
		{1000, 400, 3},
	}
	for _, c := range cases {
		if got := batchesNeeded(c.total, c.size); got != c.want {
			t.Fatalf("batchesNeeded(%d,%d)=%d want %d", c.total, c.size, got, c.want)
		}
	}
}

func TestChunkStringsPaging(t *testing.T) {
	var in []string
	for i := 0; i < 600; i++ {
		in = append(in, fmt.Sprintf("dedupe-%04d", i))
	}
	chunks := chunkStrings(in, firestoreSweepBatchSize)
	if len(chunks) != 2 || len(chunks[0]) != firestoreSweepBatchSize || len(chunks[1]) != 600-firestoreSweepBatchSize {
		t.Fatalf("600 keys at %d must split 400+200, got %v", firestoreSweepBatchSize, lens(chunks))
	}
	// Reassembly preserves every key exactly once.
	seen := map[string]int{}
	for _, c := range chunks {
		for _, k := range c {
			seen[k]++
		}
	}
	for _, k := range in {
		if seen[k] == 0 {
			t.Fatalf("key %q lost in chunking", k)
		}
	}
	if got := chunkStrings(nil, 10); len(got) != 0 {
		t.Fatalf("nil input must yield no chunks, got %v", got)
	}
}

func lens(chunks [][]string) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}
