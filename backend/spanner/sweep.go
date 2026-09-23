package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// sweepGuard re-validates, once per sweep page, that a purge still owns the
// victim incarnation it is deleting (see purge.go). Nil disables the check;
// the terminate path passes nil because the instance row still exists there,
// so no replacement incarnation can appear mid-sweep.
//
// The out-of-transaction pre-check is a cheap early exit only: the
// load-bearing fence is the in-transaction check (sweepGuardTx), which runs
// inside the same read-write transaction as the page read and delete. A
// standalone check outside the transaction leaves a gap where CreateInstance
// can recreate the ID before the delete transaction commits, and the delete
// would then remove the replacement's children. Reading the fence inside
// the transaction shares one snapshot with the page query: the delete sees
// the replacement's row iff it sees the replacement's children, so it
// either deletes only provably-old rows or aborts.
type sweepGuard func(ctx context.Context) error

// sweepGuardTx re-validates the purge fence inside the page-delete
// transaction itself (see sweepGuard). Nil disables the check.
type sweepGuardTx func(ctx context.Context, txn *spanner.ReadWriteTransaction) error

// checkGuard runs the per-page fence when one is set.
func checkGuard(ctx context.Context, guard sweepGuard) error {
	if guard == nil {
		return nil
	}
	return guard(ctx)
}

// checkGuardTx runs the in-transaction fence when one is set, before the
// page query, so the fence read and the page read share one snapshot.
func checkGuardTx(ctx context.Context, txn *spanner.ReadWriteTransaction, guardTx sweepGuardTx) error {
	if guardTx == nil {
		return nil
	}
	return guardTx(ctx, txn)
}

// Spanner commits cap buffered mutations (documented 20k; emulator and large
// rows fail earlier), so terminal paths must never buffer one commit per
// accumulated row. Sweeps delete in small paged read-write transactions.
const spannerSweepBatchSize = 500

// spannerClaimStaleDeleteCap bounds how many terminal-instance tasks one
// ClaimTasks call deletes inside its single read-write transaction. The
// refill loop re-selects while deletions free slots, so an unbounded backlog
// of stale tasks would otherwise buffer one DELETE per row in one commit and
// breach DML/mutation limits on every claim, starving live tasks behind it.
// Capping keeps each claim transaction small; the backlog drains across
// successive polls (each claim deletes up to the cap and returns any live
// tasks found within it).
const spannerClaimStaleDeleteCap = 100

// signalDedupeSweepTimeout bounds the best-effort post-commit dedupe sweep in
// CommitAdvancements. The sweep runs synchronously so a redelivered DedupeID
// inserts anew once the call returns, but a degraded store must not hold the
// caller (or a worker slot during shutdown) behind unbounded retries: on
// timeout the leftovers stay for purge and terminal notification has already
// fired (it is emitted before the sweep).
const signalDedupeSweepTimeout = 30 * time.Second

// batchesNeeded reports how many sweep batches cover total rows at the given
// batch size. It documents the chunking math behind the paged sweeps and is
// exercised by unit tests (600 dedupe rows must never fit one commit).
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
// loops page the store with LIMIT queries instead of buffering every key, but
// this helper captures the same chunking contract for unit tests.
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

// chunkInt64s splits int64 keys into consecutive batches of at most size.
func chunkInt64s(in []int64, size int) [][]int64 {
	if size <= 0 {
		size = len(in)
	}
	if len(in) == 0 || size <= 0 {
		return nil
	}
	var out [][]int64
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

// sweepTerminateDocs removes a terminated instance's task/timer/dedupe rows in
// paged transactions. The status flip already committed, so each batch is
// independent. Inbox/journal rows (if any) are left for purge: terminate never
// owned them and they need no prompt reclaim to unblock anything.
func (b *Backend) sweepTerminateDocs(ctx context.Context, id string) error {
	if err := b.deleteTasksForInstance(ctx, id, nil, nil); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id, nil, nil); err != nil {
		return err
	}
	return b.deleteAllSignalDedupe(ctx, id, nil, nil)
}

// listSignalDedupeIDs returns every dedupe key currently stored for one
// instance. It serves tests that seed keys directly; CommitAdvancements
// snapshots inside its commit transaction instead (see
// querySignalDedupeIDsTx), and the post-commit sweep then deletes exactly
// those IDs (see sweepSignalDedupeIDs).
func (b *Backend) listSignalDedupeIDs(ctx context.Context, id string) ([]string, error) {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	var out []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var k string
		if err := row.Columns(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
}

// querySignalDedupeIDsTx reads the same key set inside a read-write
// transaction. CommitAdvancements uses this so the snapshot is a
// serializable read: a SendToInbox serializing before the terminal commit is
// included in the post-commit sweep instead of lingering until purge.
func querySignalDedupeIDsTx(ctx context.Context, txn *spanner.ReadWriteTransaction, id string) ([]string, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	var out []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var k string
		if err := row.Columns(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
}

// sweepSignalDedupeIDs removes exactly the given dedupe keys in paged
// transactions. Called best-effort after terminal advancements commit with
// the in-transaction snapshot from querySignalDedupeIDsTx.
//
// Keys are classified by transaction serialization order, not client
// timestamps: a SendToInbox that captures created_at before the terminal
// commit, loses the race, and retry-commits after it would otherwise stamp
// a pre-commit time and be swept wrongly (deleting its key while the inbox
// event remains, so a later retry duplicates the signal). Snapshot IDs can
// never match such keys: only IDs visible to the terminal commit are
// removed, and keys first appearing after are preserved even when their
// client timestamp predates the commit.
//
// One residual edge (safe direction: cleanup is delayed, never wrongful): a
// send committing after the snapshot read but before the terminal commit
// lands is preserved for purge even though it logically predates the commit.
//
// The sweep stays synchronous so a redelivered DedupeID inserts anew once
// CommitAdvancements returns, but runs under a bounded context so a stuck
// store delays only this cleanup, never the caller. Purge reaps leftovers.
func (b *Backend) sweepSignalDedupeIDs(ctx context.Context, id string, guard sweepGuard, ids []string) error {
	for _, chunk := range chunkStrings(ids, spannerSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		keys := chunk
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{id, k}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteAllSignalDedupe removes every dedupe key of one instance in paged
// transactions. Used by the terminate and purge paths, where the sweep is
// awaited (terminate) or fenced (purge) and the full key set must go —
// unlike the commit path there is no post-sweep send window that snapshot
// classification needs to protect.
//
// The purge fence is validated twice per page: checkGuard outside (cheap
// early exit) and guardTx inside the delete transaction (load-bearing; see
// sweepGuard). The in-transaction check must run before the page query so
// both reads share one snapshot.
func (b *Backend) deleteAllSignalDedupe(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []string
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			keys = keys[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
			})
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var k string
				if err := row.Columns(&k); err != nil {
					return err
				}
				keys = append(keys, k)
			}
			if len(keys) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{id, k}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) < spannerSweepBatchSize {
			return nil
		}
	}
}

// deleteTasksForInstance removes wf_tasks rows for one instance in pages.
// The purge fence is validated outside (cheap early exit) and inside the
// delete transaction (load-bearing; see sweepGuard).
func (b *Backend) deleteTasksForInstance(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			keys = keys[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT id FROM wf_tasks WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
			})
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var k int64
				if err := row.Columns(&k); err != nil {
					return err
				}
				keys = append(keys, k)
			}
			if len(keys) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_tasks", spanner.Key{k}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) < spannerSweepBatchSize {
			return nil
		}
	}
}

// deleteTimersForInstance removes wf_timers rows for one instance in pages.
// The purge fence is validated outside (cheap early exit) and inside the
// delete transaction (load-bearing; see sweepGuard).
func (b *Backend) deleteTimersForInstance(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var seqs []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			seqs = seqs[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT seq FROM wf_timers WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
			})
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var s int64
				if err := row.Columns(&s); err != nil {
					return err
				}
				seqs = append(seqs, s)
			}
			if len(seqs) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, s := range seqs {
				muts = append(muts, spanner.Delete("wf_timers", spanner.Key{id, s}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(seqs) == 0 {
			return nil
		}
		if len(seqs) < spannerSweepBatchSize {
			return nil
		}
	}
}

// deleteInboxForInstance removes wf_inbox rows for one instance in pages.
// The purge fence is validated outside (cheap early exit) and inside the
// delete transaction (load-bearing; see sweepGuard).
func (b *Backend) deleteInboxForInstance(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			keys = keys[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
			})
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var k int64
				if err := row.Columns(&k); err != nil {
					return err
				}
				keys = append(keys, k)
			}
			if len(keys) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{k}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) < spannerSweepBatchSize {
			return nil
		}
	}
}

// deleteJournalForInstance removes wf_journal rows for one instance in pages.
// The purge fence is validated outside (cheap early exit) and inside the
// delete transaction (load-bearing; see sweepGuard).
func (b *Backend) deleteJournalForInstance(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var seqs []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			seqs = seqs[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT seq FROM wf_journal WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
			})
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				var s int64
				if err := row.Columns(&s); err != nil {
					return err
				}
				seqs = append(seqs, s)
			}
			if len(seqs) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, s := range seqs {
				muts = append(muts, spanner.Delete("wf_journal", spanner.Key{id, s}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(seqs) == 0 {
			return nil
		}
		if len(seqs) < spannerSweepBatchSize {
			return nil
		}
	}
}
