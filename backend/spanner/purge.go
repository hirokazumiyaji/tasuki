package spanner

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

// errPurgeSuperseded aborts a purge that no longer owns its victim: a
// concurrent purge already deleted the instance row, or the ID was recreated
// after the delete. It is translated to (false, nil) — the instance is either
// gone (counted by the winning purge) or live (must not be touched) — never
// to a caller-visible error.
var errPurgeSuperseded = errors.New("spanner: purge victim superseded")

// purgeVictim is one instance selected for purging. createdAt is the
// incarnation marker: wf_instances.created_at is written once by
// CreateInstance and never updated, so a row carrying a different value is a
// replacement created after this purge's victim was deleted.
type purgeVictim struct {
	id        string
	createdAt time.Time
}

// PurgeInstances deletes terminal instances and their dependent rows in
// per-instance paged transactions. A single transaction deleting every victim
// (or one instance's full journal/inbox) would buffer one mutation per row and
// breach the commit mutation limit, so each table is swept in
// spannerSweepBatchSize-key pages and the instance row goes last.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := nowUTC().Add(-olderThan)
	var victims []purgeVictim
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT id, created_at FROM wf_instances
			      WHERE status IN UNNEST(@sts)
			        AND completed_at IS NOT NULL AND completed_at <= @cutoff
			      ORDER BY completed_at, id LIMIT @limit`,
		Params: map[string]any{"sts": sts, "cutoff": cutoff, "limit": int64(lim)},
	})
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return 0, err
		}
		var v purgeVictim
		if err := row.Columns(&v.id, &v.createdAt); err != nil {
			iter.Stop()
			return 0, err
		}
		victims = append(victims, v)
	}
	iter.Stop()
	purged := 0
	for _, v := range victims {
		done, err := b.purgeOneInstance(ctx, v)
		if err != nil {
			return purged, err
		}
		if done {
			purged++
		}
	}
	return purged, nil
}

// purgeOneInstance removes every row of one terminal instance and reports
// whether this purge owned the delete. Child tables go first in guarded paged
// sweeps, then the instance (plus its inbox-seq counter) row goes in one
// incarnation-checked transaction, then a second guarded child sweep reaps
// writers that committed between the first sweep and the instance delete
// (they read wf_instances inside their own transaction, so post-delete
// writers abort into ErrNotFound instead).
//
// Both sweeps hold the ID-reuse fence through every page: CreateInstance may
// recreate the same ID as soon as the instance row is gone, and deletes keyed
// only by the reused ID would then corrupt the replacement. The first sweep
// proceeds only while the instance row still carries the listed created_at;
// the delete commits only for that same incarnation (a concurrent purge that
// deleted first, or a replacement created since, makes this purge stand down
// uncounted); the second sweep proceeds only while the row stays absent. When
// the second sweep stops at a replacement, residual stragglers snapshotted
// before the delete are still reaped by exact key (see
// listResidualStragglers) instead of leaking into the
// replacement. Stragglers that cannot be proven old leak, and are reaped with
// the replacement's own purge once it is terminal — leaked rows are always
// preferable to deleting a live incarnation's rows.
func (b *Backend) purgeOneInstance(ctx context.Context, v purgeVictim) (bool, error) {
	own := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, v) }
	ownTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, v)
	}
	if err := b.deleteInstanceChildren(ctx, v.id, own, ownTx); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return false, nil
		}
		return false, err
	}
	// The residual snapshot is taken inside the victim-delete transaction
	// below (deletePurgedInstanceRow), not by a separate listing: the
	// victim-delete transaction is the serialization point against an
	// ID-reusing CreateInstance (which can only insert once the victim row
	// is gone), so any key the snapshot observes provably predates any
	// replacement by transaction order, not by wall-clock comparison.
	// Stragglers that commit concurrently with the delete and serialize
	// after it are excluded from the snapshot and leak safely; they are
	// reaped with the replacement's own purge once it is terminal. Leaked
	// rows are always preferable to deleting a live incarnation's rows.
	deleted, residual, err := b.deletePurgedInstanceRow(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	gone := func(ctx context.Context) error { return b.checkPurgeAbsent(ctx, v.id) }
	goneTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeAbsentTx(ctx, txn, v.id)
	}
	if err := b.deleteInstanceChildren(ctx, v.id, gone, goneTx); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			// A replacement incarnation appeared after this purge deleted
			// the victim: reap the residual rows snapshotted above by exact
			// key instead of leaking them. This purge still owns the victim
			// delete, so it stays counted.
			if rerr := b.reapResidualStragglers(ctx, v.id, residual); rerr != nil {
				return true, rerr
			}
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// checkPurgeVictim enforces the first-sweep fence: the victim row must still
// exist with the incarnation observed at listing time. A missing row means a
// concurrent purge already deleted it; a different created_at means the ID
// was recreated after such a delete. Either way the sweep must stop before it
// touches another incarnation's rows.
func (b *Backend) checkPurgeVictim(ctx context.Context, v purgeVictim) error {
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at"})
	if isNotFound(err) {
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	var createdAt time.Time
	if err := row.Columns(&createdAt); err != nil {
		return err
	}
	if !createdAt.Equal(v.createdAt) {
		return errPurgeSuperseded
	}
	return nil
}

// checkPurgeAbsent enforces the second-sweep fence: the victim row must stay
// gone. Any row present now is a replacement incarnation, so the sweep stops
// before deleting its children.
func (b *Backend) checkPurgeAbsent(ctx context.Context, id string) error {
	_, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"id"})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return errPurgeSuperseded
}

// checkPurgeVictimTx enforces the first-sweep fence inside a page-delete
// transaction: the victim row must still exist with the incarnation observed
// at listing time. Reading the fence inside the transaction shares one
// snapshot with the page query, so a CreateInstance that recreates the ID
// is either invisible to both (only old rows are deleted) or visible to
// both (the fence trips and the transaction aborts before deleting
// anything). A missing row means a concurrent purge already deleted it; a
// different created_at means the ID was recreated after such a delete.
func (b *Backend) checkPurgeVictimTx(ctx context.Context, txn *spanner.ReadWriteTransaction, v purgeVictim) error {
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at"})
	if isNotFound(err) {
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	var createdAt time.Time
	if err := row.Columns(&createdAt); err != nil {
		return err
	}
	if !createdAt.Equal(v.createdAt) {
		return errPurgeSuperseded
	}
	return nil
}

// checkPurgeAbsentTx enforces the second-sweep fence inside a page-delete
// transaction: the victim row must stay gone. Any row present in the same
// snapshot as the page query is a replacement incarnation, so the sweep
// stops before deleting its children.
func (b *Backend) checkPurgeAbsentTx(ctx context.Context, txn *spanner.ReadWriteTransaction, id string) error {
	_, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"id"})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return errPurgeSuperseded
}

// deletePurgedInstanceRow removes the victim row (plus its inbox-seq counter)
// only if it still carries the listed incarnation. A concurrent purge that
// won the delete, or a replacement created since, yields (false, nil, nil):
// this purge owns nothing and must neither sweep further nor count the
// instance.
//
// On a successful delete it also returns the residual snapshot: the dedupe
// and inbox keys observed inside this same transaction. The delete
// transaction is the serialization point against an ID-reusing
// CreateInstance (which inserts the replacement only after this row is
// gone), so every snapshotted key provably predates any replacement —
// atomically, with no listing-to-delete gap for a terminal SendToInbox to
// slip a row into that the exact-reap could then mistake for the
// replacement's. Rows that serialize after this transaction are excluded
// and leak safely for the replacement's own purge.
func (b *Backend) deletePurgedInstanceRow(ctx context.Context, v purgeVictim) (bool, *residualStragglers, error) {
	deleted := false
	var residual residualStragglers
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Reset per attempt: the transaction function may run more than
		// once, and only the committing attempt's snapshot classifies the
		// residual reap.
		deleted = false
		residual = residualStragglers{}
		row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at"})
		if isNotFound(err) {
			return errPurgeSuperseded
		}
		if err != nil {
			return err
		}
		var createdAt time.Time
		if err := row.Columns(&createdAt); err != nil {
			return err
		}
		if !createdAt.Equal(v.createdAt) {
			return errPurgeSuperseded
		}
		if err := queryResidualDedupeTx(ctx, txn, v.id, &residual); err != nil {
			return err
		}
		if err := queryResidualInboxTx(ctx, txn, v.id, &residual); err != nil {
			return err
		}
		deleted = true
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{v.id}),
			spanner.Delete("wf_instances", spanner.Key{v.id}),
		})
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return deleted, &residual, nil
}

func (b *Backend) deleteInstanceChildren(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	if err := b.deleteTasksForInstance(ctx, id, guard, guardTx); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id, guard, guardTx); err != nil {
		return err
	}
	if err := b.deleteAllSignalDedupe(ctx, id, guard, guardTx); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id, guard, guardTx); err != nil {
		return err
	}
	return b.deleteJournalForInstance(ctx, id, guard, guardTx)
}

// residualStragglers holds exact keys observed after the first sweep but
// before the victim delete (see purgeOneInstance). Deleting by key avoids
// cross-process wall-clock skew: created_at stamps from different nodes
// cannot be compared reliably, but a key listed before the delete provably
// predates any replacement created after it.
//
// Dedupe keys additionally carry the created_at observed at listing: dedupe
// keys are deterministic ((instance_id, dedupe_id)), so a replacement
// incarnation can recreate the very same row between the snapshot and the
// reap. The reap deletes a dedupe row only when its created_at still matches
// the snapshot (dedupe rows are create-once, never updated, so any difference
// proves the row is the replacement's, not the straggler's). Inbox rows use
// random IDs a replacement cannot reuse, so exact-key deletes stay
// unconditional for them.
type residualStragglers struct {
	dedupe   []dedupeVersion
	inboxIDs []int64
}

// dedupeVersion pins one snapshotted dedupe row: its key plus the created_at
// observed at snapshot time (see residualStragglers).
type dedupeVersion struct {
	id        string
	createdAt time.Time
}

// listResidualStragglers snapshots the dedupe and inbox keys that survived
// the first sweep (stragglers committed during the sweep). Only these two
// tables can gain rows while the victim is terminal. Production purge takes
// this snapshot inside the victim-delete transaction instead (see
// deletePurgedInstanceRow); this standalone listing serves tests that seed
// keys directly.
func (b *Backend) listResidualStragglers(ctx context.Context, id string) (*residualStragglers, error) {
	var out residualStragglers
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id, created_at FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return nil, err
		}
		var k string
		var ts time.Time
		if err := row.Columns(&k, &ts); err != nil {
			iter.Stop()
			return nil, err
		}
		out.dedupe = append(out.dedupe, dedupeVersion{id: k, createdAt: ts})
	}
	iter.Stop()
	iter2 := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	for {
		row, err := iter2.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter2.Stop()
			return nil, err
		}
		var k int64
		if err := row.Columns(&k); err != nil {
			iter2.Stop()
			return nil, err
		}
		out.inboxIDs = append(out.inboxIDs, k)
	}
	iter2.Stop()
	return &out, nil
}

// queryResidualDedupeTx reads the victim's dedupe keys inside the
// victim-delete transaction for the atomic residual snapshot (see
// deletePurgedInstanceRow). Only these two tables can gain rows while the
// victim is terminal — SendToInbox inserts both.
func queryResidualDedupeTx(ctx context.Context, txn *spanner.ReadWriteTransaction, id string, out *residualStragglers) error {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id, created_at FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		var k string
		var ts time.Time
		if err := row.Columns(&k, &ts); err != nil {
			return err
		}
		out.dedupe = append(out.dedupe, dedupeVersion{id: k, createdAt: ts})
	}
}

// queryResidualInboxTx reads the victim's inbox keys inside the
// victim-delete transaction for the atomic residual snapshot (see
// deletePurgedInstanceRow).
func queryResidualInboxTx(ctx context.Context, txn *spanner.ReadWriteTransaction, id string, out *residualStragglers) error {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT id FROM wf_inbox WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		var k int64
		if err := row.Columns(&k); err != nil {
			return err
		}
		out.inboxIDs = append(out.inboxIDs, k)
	}
}

// reapResidualStragglers deletes the snapshotted residual keys. Inbox keys
// (random IDs) go by exact key in paged transactions: keys already gone are
// skipped, keys never snapshotted (replacement rows and post-snapshot
// stragglers) are preserved.
//
// Dedupe keys are deleted conditionally, one atomic DML statement per key
// predicated on the snapshotted created_at: a replacement incarnation
// reusing the same DedupeID after the snapshot stamps a different
// created_at (dedupe rows are create-once, never updated), so the statement
// matches zero rows and the replacement's live guard survives (Codex round 6
// on #327). Deleting it unconditionally would strip the guard while the
// replacement's inbox event remains, duplicating a later retry. A straggler
// the replacement inherited (deduped against, so no inbox event of its own)
// still carries the snapshotted stamp and is reaped, letting a retry insert
// anew.
func (b *Backend) reapResidualStragglers(ctx context.Context, id string, r *residualStragglers) error {
	if r == nil {
		return nil
	}
	// Datastore errors from either loop propagate to the caller: purge must
	// report failure rather than success while a stale dedupe row stays
	// attached (a later SendToInbox for that DedupeID would find the stale
	// row and drop the event). Only version-mismatch is benign — the
	// predicated delete then matches zero rows, which commits successfully
	// and preserves the replacement's recreated guard.
	for _, dv := range r.dedupe {
		if err := ctx.Err(); err != nil {
			return err
		}
		dv := dv
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, spanner.Statement{
				SQL:    `DELETE FROM wf_signal_dedupe WHERE instance_id = @id AND dedupe_id = @k AND created_at = @ts`,
				Params: map[string]any{"id": id, "k": dv.id, "ts": dv.createdAt},
			})
			return err
		}); err != nil {
			return err
		}
	}
	for _, chunk := range chunkInt64s(r.inboxIDs, spannerSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys := chunk
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{k}))
			}
			return txn.BufferWrite(muts)
		}); err != nil {
			return err
		}
	}
	return nil
}
