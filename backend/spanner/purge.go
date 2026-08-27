package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

// PurgeInstances deletes terminal instances and their dependent rows inside a
// read-write transaction.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := nowUTC().Add(-olderThan)
	var ids []string
	err = b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ids = ids[:0]
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT id FROM wf_instances
			      WHERE status IN UNNEST(@sts)
			        AND completed_at IS NOT NULL AND completed_at <= @cutoff
			      ORDER BY completed_at, id LIMIT @limit`,
			Params: map[string]any{"sts": sts, "cutoff": cutoff, "limit": int64(lim)},
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
			var id string
			if err := row.Column(0, &id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			return nil
		}
		for _, table := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
			if _, err := txn.Update(ctx, spanner.Statement{
				SQL:    `DELETE FROM ` + table + ` WHERE instance_id IN UNNEST(@ids)`,
				Params: map[string]any{"ids": ids},
			}); err != nil {
				return err
			}
		}
		_, err := txn.Update(ctx, spanner.Statement{
			SQL:    `DELETE FROM wf_instances WHERE id IN UNNEST(@ids)`,
			Params: map[string]any{"ids": ids},
		})
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}
