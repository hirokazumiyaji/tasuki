package firestore

import (
	"context"

	"google.golang.org/api/iterator"
)

// Firestore caps a transaction (and a batched write) at 500 operations.
// Terminal paths must never scale one commit with the instance's accumulated
// rows, so sweeps page deletes well below the cap.
const (
	firestoreTxWriteLimit   = 500
	firestoreSweepBatchSize = 400
)

// batchesNeeded reports how many sweep batches cover total rows at the given
// batch size. It documents the chunking math behind the paged sweeps below
// and is exercised by unit tests (600 dedupe rows must never fit one commit).
func batchesNeeded(total, size int) int {
	if total <= 0 {
		return 0
	}
	if size <= 0 {
		return total
	}
	return (total + size - 1) / size
}

// chunkStrings splits keys into consecutive batches of at most size. Sweep
// loops page the store with Limit queries instead of buffering every key, but
// this helper captures the same chunking contract for unit tests and for any
// future collect-then-delete path.
func chunkStrings(in []string, size int) [][]string {
	if size <= 0 {
		size = len(in)
	}
	if len(in) == 0 || size <= 0 {
		return nil
	}
	var out [][]string
	for len(in) > 0 {
		n := size
		if n > len(in) {
			n = len(in)
		}
		out = append(out, in[:n])
		in = in[n:]
	}
	return out
}

// sweepTerminateDocs removes the mutable child documents of a terminated
// instance in paged batches. The status flip already committed, so each batch
// is an independent non-transactional commit.
func (b *Backend) sweepTerminateDocs(ctx context.Context, id string) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe"} {
		if err := b.deleteDocsByInstance(ctx, col, id); err != nil {
			return err
		}
	}
	return nil
}

// sweepSignalDedupe removes an instance's dedupe keys in paged batches.
// Called best-effort after terminal advancements commit.
func (b *Backend) sweepSignalDedupe(ctx context.Context, id string) error {
	return b.deleteDocsByInstance(ctx, "wf_signal_dedupe", id)
}

// deleteDocsByInstance deletes every document in col with instance_id == id,
// one Limit-sized batch commit at a time. The loop re-queries until a page
// comes back empty, so arbitrarily many rows converge without ever buffering
// them all or exceeding the write cap in one commit.
func (b *Backend) deleteDocsByInstance(ctx context.Context, col, id string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		it := b.col(col).Where("instance_id", "==", id).Limit(firestoreSweepBatchSize).Documents(ctx)
		batch := b.client.Batch()
		n := 0
		for {
			dsnap, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				it.Stop()
				return err
			}
			batch.Delete(dsnap.Ref)
			n++
		}
		it.Stop()
		if n == 0 {
			return nil
		}
		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
		if n < firestoreSweepBatchSize {
			return nil
		}
	}
}
