package spanner

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// sweepGuard re-validates, once per sweep page, that a sweep still owns the
// incarnation it is deleting (see purge.go). Purge arms it with the listed
// victim; terminal sweeps arm it with the pre-commit incarnation (see
// sweepTerminateDocs) — the instance row still exists on the terminal path,
// but a purge interleaved with the paused sweep can delete it and let
// CreateInstance reuse the ID, so the fence is load-bearing there too.
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
// fenced paged transactions. The status flip already committed, so each page
// is independent — but every page still re-validates the terminal fence (see
// below): a PurgeInstances that deletes the terminal instance, clears its
// marker, and lets CreateInstance reuse the ID while this sweep is paused
// must stop the resumed sweep before it deletes the replacement's rows
// (Codex round 20 on #296). The fence pins the pre-commit incarnation (id +
// created_at captured inside the flip transaction); on mismatch the sweep
// aborts with a nil return and purge owns the leftovers — leaked rows are
// always preferable to deleting a live incarnation's rows. Inbox/journal rows
// (if any) are left for purge: terminate never owned them and they need no
// prompt reclaim to unblock anything. Dedupe cleanup deletes only the
// pre-termination non-marker keys snapshotted inside the flip transaction:
// post-terminal sends committing after the flip survive, and markers in the
// snapshot itself are filtered out (Codex round 8 on #327: sweeping a marker
// while its inbox event remains duplicates the next retry). Purge reaps
// leftovers.
func (b *Backend) sweepTerminateDocs(ctx context.Context, victim purgeVictim, dedupeSnapshot []string) error {
	// The present-incarnation fence (see purgeVictim): the sweep proceeds
	// only while the instance row still carries the incarnation captured at
	// the terminal commit. A missing row (purge deleted it) or a different
	// incarnation (the ID was recreated after such a delete) aborts the
	// sweep before it touches another incarnation's rows. The token
	// comparison (see victimMatches) survives clock rollback, VM restore,
	// and timestamp truncation that can all reproduce the same created_at.
	id := victim.id
	guard := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, victim) }
	guardTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, victim)
	}
	if err := b.deleteTasksForInstance(ctx, id, guard, guardTx); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return nil
		}
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id, guard, guardTx); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return nil
		}
		return err
	}
	return b.sweepSignalDedupeIDs(ctx, id, guard, guardTx, filterTerminateDedupeKeys(dedupeSnapshot))
}

// filterTerminateDedupeKeys keeps only the pre-termination non-marker keys of
// a terminate snapshot (Codex round 8 on #327). Pure for unit tests.
func filterTerminateDedupeKeys(keys []string) []string {
	var out []string
	for _, k := range keys {
		if !isPostTerminalMarkerKey(k) {
			out = append(out, k)
		}
	}
	return out
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

// terminalSweep carries one terminal advancement's post-commit sweep: the
// pre-commit incarnation fencing it plus the exact dedupe keys to remove.
type terminalSweep struct {
	createdAt   time.Time
	incarnation string
	dedupeIDs   []string
}

// queryInstanceVictimTx reads an instance's fence identity inside the commit
// transaction for the terminal sweep fence (see terminalSweep): the
// created_at marker plus the unique incarnation token (see newIncarnation).
// created_at is written once by CreateInstance and never updated, so a row
// carrying a different value after a purge is a replacement incarnation —
// as is a row carrying a different token when created_at coincides through
// clock rollback, VM restore, or precision truncation.
func queryInstanceVictimTx(ctx context.Context, txn *spanner.ReadWriteTransaction, id string) (purgeVictim, error) {
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at", "incarnation"})
	if err != nil {
		return purgeVictim{}, err
	}
	v := purgeVictim{id: id}
	var incarnation spanner.NullString
	if err := row.Columns(&v.createdAt, &incarnation); err != nil {
		return purgeVictim{}, err
	}
	if incarnation.Valid {
		v.incarnation = incarnation.StringVal
	}
	return v, nil
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

// sweepSignalDedupeIDs removes exactly the given dedupe keys in fenced paged
// transactions. Called best-effort after terminal advancements commit with
// the in-transaction snapshot from querySignalDedupeIDsTx. Terminal-only:
// purge reaps by full key listing instead (see deleteAllSignalDedupe), so a
// tripped fence aborts with a nil return and purge owns the leftovers.
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
// Every page runs in one read-write transaction that first re-validates the
// terminal fence (the instance row must still carry the pre-commit
// incarnation) and then deletes the page: dedupe keys are deterministic
// ((instance_id, dedupe_id)), so a replacement incarnation reusing the ID
// after a purge can recreate the very same row. An unfenced exact-key delete
// would then strip the replacement's live guard while its inbox event
// remains, duplicating a later retry (Codex round 20 on #296). The fence
// read and the deletes share the transaction, so a recreation is either
// invisible to both (only provably-old rows are deleted) or visible to both
// (the fence trips and the page aborts).
//
// The sweep stays synchronous so a redelivered DedupeID inserts anew once
// CommitAdvancements returns, but runs under a bounded context so a stuck
// store delays only this cleanup, never the caller. Purge reaps leftovers.
func (b *Backend) sweepSignalDedupeIDs(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx, ids []string) error {
	for _, chunk := range chunkStrings(ids, spannerSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			if errors.Is(err, errPurgeSuperseded) {
				return nil
			}
			return err
		}
		keys := chunk
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{id, k}))
			}
			return txn.BufferWrite(muts)
		})
		if errors.Is(err, errPurgeSuperseded) {
			return nil
		}
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
