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
	if err := b.deleteInstanceChildren(ctx, v.id, own); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return false, nil
		}
		return false, err
	}
	// Snapshot residual rows observed after the first sweep but before the
	// victim delete. Any key listed here predates the replacement (which can
	// only appear after the delete below), so it is provably old by
	// transaction order, not by wall-clock comparison. For inbox rows (random
	// IDs) the replacement cannot reuse the same key; for dedupe keys,
	// deleting a shared ID fixes the inheritance (the replacement's send was
	// deduped against the straggler, so removing the key lets a retry insert
	// anew). Stragglers committing after this listing leak safely.
	residual, rerr := b.listResidualStragglers(ctx, v.id)
	if rerr != nil {
		return false, rerr
	}
	deleted, err := b.deletePurgedInstanceRow(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	gone := func(ctx context.Context) error { return b.checkPurgeAbsent(ctx, v.id) }
	if err := b.deleteInstanceChildren(ctx, v.id, gone); err != nil {
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

// deletePurgedInstanceRow removes the victim row (plus its inbox-seq counter)
// only if it still carries the listed incarnation. A concurrent purge that
// won the delete, or a replacement created since, yields (false, nil): this
// purge owns nothing and must neither sweep further nor count the instance.
func (b *Backend) deletePurgedInstanceRow(ctx context.Context, v purgeVictim) (bool, error) {
	deleted := false
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
		deleted = true
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{v.id}),
			spanner.Delete("wf_instances", spanner.Key{v.id}),
		})
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func (b *Backend) deleteInstanceChildren(ctx context.Context, id string, guard sweepGuard) error {
	if err := b.deleteTasksForInstance(ctx, id, guard); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id, guard); err != nil {
		return err
	}
	if err := b.deleteAllSignalDedupe(ctx, id, guard); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id, guard); err != nil {
		return err
	}
	return b.deleteJournalForInstance(ctx, id, guard)
}

// residualStragglers holds exact keys observed after the first sweep but
// before the victim delete (see purgeOneInstance). Deleting by key avoids
// cross-process wall-clock skew: created_at stamps from different nodes
// cannot be compared reliably, but a key listed before the delete provably
// predates any replacement created after it.
type residualStragglers struct {
	dedupeIDs []string
	inboxIDs  []int64
}

// listResidualStragglers snapshots the dedupe and inbox keys that survived
// the first sweep (stragglers committed during the sweep). Only these two
// tables can gain rows while the victim is terminal.
func (b *Backend) listResidualStragglers(ctx context.Context, id string) (*residualStragglers, error) {
	var out residualStragglers
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id`,
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
		if err := row.Columns(&k); err != nil {
			iter.Stop()
			return nil, err
		}
		out.dedupeIDs = append(out.dedupeIDs, k)
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

// reapResidualStragglers deletes the snapshotted residual keys by exact key
// in paged transactions. Keys already gone are skipped; keys never
// snapshotted (replacement rows and post-snapshot stragglers) are preserved.
func (b *Backend) reapResidualStragglers(ctx context.Context, id string, r *residualStragglers) error {
	if r == nil {
		return nil
	}
	for _, chunk := range chunkStrings(r.dedupeIDs, spannerSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys := chunk
		if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{id, k}))
			}
			return txn.BufferWrite(muts)
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
