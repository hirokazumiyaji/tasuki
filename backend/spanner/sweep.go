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
type sweepGuard func(ctx context.Context) error

// checkGuard runs the per-page fence when one is set.
func checkGuard(ctx context.Context, guard sweepGuard) error {
	if guard == nil {
		return nil
	}
	return guard(ctx)
}

// Spanner commits cap buffered mutations (documented 20k; emulator and large
// rows fail earlier), so terminal paths must never buffer one commit per
// accumulated row. Sweeps delete in small paged read-write transactions.
const spannerSweepBatchSize = 500

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
	if err := b.deleteTasksForInstance(ctx, id, nil); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id, nil); err != nil {
		return err
	}
	return b.sweepSignalDedupe(ctx, id, nil)
}

// sweepSignalDedupe removes an instance's dedupe keys in paged transactions.
// Called best-effort after terminal advancements commit.
func (b *Backend) sweepSignalDedupe(ctx context.Context, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []string
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
func (b *Backend) deleteTasksForInstance(ctx context.Context, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
func (b *Backend) deleteTimersForInstance(ctx context.Context, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var seqs []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
func (b *Backend) deleteInboxForInstance(ctx context.Context, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
func (b *Backend) deleteJournalForInstance(ctx context.Context, id string, guard sweepGuard) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var seqs []int64
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
