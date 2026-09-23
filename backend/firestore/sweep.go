package firestore

import (
	"context"
	"time"

	"google.golang.org/api/iterator"
)

// Firestore caps a transaction (and a batched write) at 500 operations.
// Terminal paths must never scale one commit with the instance's accumulated
// rows, so sweeps page deletes well below the cap.
const (
	firestoreTxWriteLimit   = 500
	firestoreSweepBatchSize = 400
)

// signalDedupeSweepTimeout bounds the best-effort post-commit dedupe sweep in
// CommitAdvancements. The sweep runs synchronously so a redelivered DedupeID
// inserts anew once the call returns, but a degraded store must not hold the
// caller (or a worker slot during shutdown) behind unbounded retries: on
// timeout the leftovers stay for purge and terminal notification has already
// fired (it is emitted before the sweep).
const signalDedupeSweepTimeout = 30 * time.Second

// sweepGuard re-validates, once per sweep page, that a purge still owns the
// victim incarnation it is deleting (see purge.go). Nil disables the check;
// the terminate path passes nil because the instance doc still exists there,
// so no replacement incarnation can appear mid-sweep.
type sweepGuard func(ctx context.Context) error

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
		if err := b.deleteDocsByInstance(ctx, col, id, nil); err != nil {
			return err
		}
	}
	return nil
}

// sweepSignalDedupe removes an instance's dedupe keys created at or before
// the terminal commit (cutoff), in paged batches. Called best-effort after
// terminal advancements commit.
//
// Only pre-commit keys are removed: notifyTerminal fires before this sweep,
// so a concurrent SendToInbox with a new DedupeID can land inside the sweep
// window. Deleting that key while its inbox event remains would let a later
// retry of the same DedupeID duplicate the signal, so keys stamped after the
// terminal commit are left for purge. The bound is applied client-side: an
// instance_id + created_at server-side query would need a composite index
// (see purge.go). Docs without created_at predate the stamping and are
// removed.
//
// The loop stops at the first page with nothing deletable: post-commit keys
// never shrink the result set, so re-querying until empty could spin while
// sends keep arriving. Residual pre-commit keys stranded behind a page of
// newer keys stay for purge (the same best-effort model as the sweep
// timeout above).
func (b *Backend) sweepSignalDedupe(ctx context.Context, id string, cutoff time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		it := b.col("wf_signal_dedupe").Where("instance_id", "==", id).Limit(firestoreSweepBatchSize).Documents(ctx)
		batch := b.client.Batch()
		n, deletable := 0, 0
		for {
			dsnap, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				it.Stop()
				return err
			}
			n++
			if ts := timestamp(dsnap.Data(), "created_at"); ts.IsZero() || !ts.After(cutoff) {
				batch.Delete(dsnap.Ref)
				deletable++
			}
		}
		it.Stop()
		if deletable == 0 {
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

// deleteDocsByInstance deletes every document in col with instance_id == id,
// one Limit-sized batch commit at a time. The loop re-queries until a page
// comes back empty, so arbitrarily many rows converge without ever buffering
// them all or exceeding the write cap in one commit. A purge passes a guard
// holding the ID-reuse fence through every page; a tripped guard aborts the
// sweep so a replacement incarnation's documents are never deleted.
func (b *Backend) deleteDocsByInstance(ctx context.Context, col, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if guard != nil {
			if err := guard(ctx); err != nil {
				return err
			}
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
