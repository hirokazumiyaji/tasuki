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

// listSignalDedupeIDs returns the document IDs of every dedupe key
// currently stored for one instance. CommitAdvancements snapshots this set
// immediately before the terminal commit transaction; the post-commit sweep
// then deletes exactly those IDs (see sweepSignalDedupeIDs).
func (b *Backend) listSignalDedupeIDs(ctx context.Context, id string) ([]string, error) {
	it := b.col("wf_signal_dedupe").Where("instance_id", "==", id).Documents(ctx)
	var out []string
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			it.Stop()
			return nil, err
		}
		out = append(out, dsnap.Ref.ID)
	}
	it.Stop()
	return out, nil
}

// sweepSignalDedupeIDs removes exactly the given dedupe documents in paged
// batches. Called best-effort after terminal advancements commit with the
// pre-commit snapshot from listSignalDedupeIDs.
//
// Keys are classified by transaction serialization order, not client
// timestamps: a SendToInbox that captures created_at before the terminal
// commit, loses the race, and retry-commits after it would otherwise stamp
// a pre-commit time and be swept wrongly (deleting its key while the inbox
// event remains, so a later retry duplicates the signal). Snapshot IDs can
// never match such keys: only IDs visible before the terminal commit are
// removed, and keys first appearing after are preserved even when their
// client timestamp predates the commit.
//
// Two boundary caveats (round 2 equality-edge and stranded-page, carried
// forward):
//   - Snapshot edge: a send serializing between the snapshot listing and
//     the terminal commit is preserved for purge even though it logically
//     predates the commit. Safe direction: cleanup is delayed, never
//     wrongful.
//   - Listing race: a concurrent send landing mid-listing can be missed by
//     the snapshot and likewise stays for purge.
//
// The sweep stays synchronous so a redelivered DedupeID inserts anew once
// CommitAdvancements returns, but runs under a bounded context so a stuck
// store delays only this cleanup, never the caller. Purge reaps leftovers.
func (b *Backend) sweepSignalDedupeIDs(ctx context.Context, ids []string) error {
	for _, chunk := range chunkStrings(ids, firestoreSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := b.client.Batch()
		for _, docID := range chunk {
			batch.Delete(b.ref("wf_signal_dedupe", docID))
		}
		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
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
