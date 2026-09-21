package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

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
	var ids []string
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT id FROM wf_instances
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
		var id string
		if err := row.Column(0, &id); err != nil {
			iter.Stop()
			return 0, err
		}
		ids = append(ids, id)
	}
	iter.Stop()
	purged := 0
	for _, id := range ids {
		if err := b.purgeOneInstance(ctx, id); err != nil {
			return purged, err
		}
		purged++
	}
	return purged, nil
}

// purgeOneInstance removes every row of one terminal instance. Child tables go
// first in paged sweeps, then the instance (plus its inbox-seq counter) row,
// then a second child sweep reaps writers that committed between the first
// sweep and the instance delete (they read wf_instances inside their own
// transaction, so post-delete writers abort into ErrNotFound instead).
//
// The trailing sweep is fenced on instance-ID reuse: CreateInstance may
// recreate the same ID as soon as the instance row is gone, and an
// unconditional second sweep would then delete the replacement's tasks,
// timers, inbox, journal and seq rows. When the instance row reappears after
// the delete, the second sweep is skipped so a live replacement is never
// corrupted (at most a few straggler rows from the purged incarnation leak,
// and they are reaped with the replacement's own purge once it is terminal).
func (b *Backend) purgeOneInstance(ctx context.Context, id string) error {
	if err := b.deleteInstanceChildren(ctx, id); err != nil {
		return err
	}
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{id}),
			spanner.Delete("wf_instances", spanner.Key{id}),
		})
	})
	if err != nil {
		return err
	}
	if _, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"id"}); err == nil {
		return nil
	} else if !isNotFound(err) {
		return err
	}
	return b.deleteInstanceChildren(ctx, id)
}

func (b *Backend) deleteInstanceChildren(ctx context.Context, id string) error {
	if err := b.deleteTasksForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.deleteTimersForInstance(ctx, id); err != nil {
		return err
	}
	if err := b.sweepSignalDedupe(ctx, id); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id); err != nil {
		return err
	}
	return b.deleteJournalForInstance(ctx, id)
}
