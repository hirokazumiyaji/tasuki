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
// the second sweep stops at a replacement, provably-old stragglers are still
// reaped (see reapReplacedStragglers) instead of leaking into the
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
			// the victim: an inbox row (or dedupe key) that committed
			// between the first sweep and the delete belongs to the old
			// incarnation, but the fence above leaves it for the
			// replacement's LoadWorkflow to consume. Reap the rows that
			// provably predate the replacement instead of leaking them.
			// This purge still owns the victim delete, so it stays counted.
			if rerr := b.reapReplacedStragglers(ctx, v.id); rerr != nil {
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

// reapReplacedStragglers deletes child rows that provably belong to the
// purged incarnation after the second sweep stopped at a replacement (see
// purgeOneInstance). Only wf_signal_dedupe and wf_inbox can gain rows while
// the victim is terminal — SendToInbox inserts both and wakes nothing once
// the instance is terminal. Task, timer and journal rows are written only on
// behalf of the running victim, so the aborted sweep left none of those
// behind and their tables are not reaped here.
func (b *Backend) reapReplacedStragglers(ctx context.Context, id string) error {
	if err := b.reapStragglerDedupe(ctx, id); err != nil {
		return err
	}
	return b.reapStragglerInbox(ctx, id)
}

// stragglerCutoff resolves the current incarnation: absent reports no
// instance row (every row with the ID is old); otherwise cutoff is the
// replacement's created_at and only rows stamped strictly before it are old.
// Both stamps come from the same clock, and a straggler committed before the
// victim delete strictly predates a replacement created after it. Rows
// without a usable stamp, or tied with the replacement, are preserved:
// leaking is always preferable to deleting a live incarnation's rows.
func (b *Backend) stragglerCutoff(ctx context.Context, id string) (cutoff time.Time, absent bool, err error) {
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at"})
	if isNotFound(err) {
		return time.Time{}, true, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	var createdAt time.Time
	if err := row.Columns(&createdAt); err != nil {
		return time.Time{}, false, err
	}
	return createdAt, false, nil
}

func (b *Backend) reapStragglerDedupe(ctx context.Context, id string) error {
	type key struct {
		dedupeID  string
		createdAt time.Time
	}
	var pending []key
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		cutoff, absent, err := b.stragglerCutoff(ctx, id)
		if err != nil {
			return err
		}
		var muts []*spanner.Mutation
		for _, k := range pending {
			if !absent && (k.createdAt.IsZero() || !k.createdAt.Before(cutoff)) {
				continue
			}
			muts = append(muts, spanner.Delete("wf_signal_dedupe", spanner.Key{id, k.dedupeID}))
		}
		pending = pending[:0]
		if len(muts) == 0 {
			return nil
		}
		return b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite(muts)
		})
	}
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT dedupe_id, created_at FROM wf_signal_dedupe WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
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
		var k key
		if err := row.Columns(&k.dedupeID, &k.createdAt); err != nil {
			return err
		}
		pending = append(pending, k)
		if len(pending) == spannerSweepBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (b *Backend) reapStragglerInbox(ctx context.Context, id string) error {
	type key struct {
		rowID     int64
		createdAt time.Time
	}
	var pending []key
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		cutoff, absent, err := b.stragglerCutoff(ctx, id)
		if err != nil {
			return err
		}
		var muts []*spanner.Mutation
		for _, k := range pending {
			if !absent && (k.createdAt.IsZero() || !k.createdAt.Before(cutoff)) {
				continue
			}
			muts = append(muts, spanner.Delete("wf_inbox", spanner.Key{k.rowID}))
		}
		pending = pending[:0]
		if len(muts) == 0 {
			return nil
		}
		return b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite(muts)
		})
	}
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT id, created_at FROM wf_inbox WHERE instance_id = @id`,
		Params: map[string]any{"id": id},
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
		var k key
		if err := row.Columns(&k.rowID, &k.createdAt); err != nil {
			return err
		}
		pending = append(pending, k)
		if len(pending) == spannerSweepBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
