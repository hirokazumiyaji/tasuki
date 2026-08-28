package firestore

import (
	"context"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

// purgeStatusSet filters terminal instances client-side. Only completed_at is
// queried server-side so no composite index is required.
func purgeStatusSet(sts []string) map[string]struct{} {
	set := make(map[string]struct{}, len(sts))
	for _, s := range sts {
		set[s] = struct{}{}
	}
	return set
}

// PurgeInstances removes terminal instances older than the retention window
// together with their tasks, timers, dedupe entries, inbox items, journal and
// inbox sequence counters. Documents are removed in batches; concurrent
// progress on the same instance is not possible (terminal instances are
// immutable), so best-effort batching is safe.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	statuses2 := purgeStatusSet(sts)
	cutoff := nowUTC().Add(-olderThan)

	var ids []string
	it := b.col("wf_instances").Where("completed_at", "<=", cutoff).Documents(ctx)
	defer it.Stop()
	for len(ids) < lim {
		snap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		if _, ok := statuses2[str(snap.Data(), "status")]; !ok {
			continue
		}
		ids = append(ids, snap.Ref.ID)
	}

	purged := 0
	for _, id := range ids {
		if err := b.purgeInstanceDocs(ctx, id); err != nil {
			return purged, err
		}
		// Inbox writers read wf_instances inside their transaction, so once
		// this delete commits no new child documents can appear for the
		// instance (in-flight writers lose the race and retry into
		// ErrNotFound). A writer that committed between the sweep above and
		// this delete is reaped by the second pass below.
		if _, err := b.ref("wf_instances", id).Delete(ctx); err != nil {
			return purged, err
		}
		if err := b.purgeInstanceDocs(ctx, id); err != nil {
			return purged, err
		}
		purged++
	}
	return purged, nil
}

// purgeInstanceDocs removes every child document of one instance. Batches
// commit while iterating so a large journal or inbox never buffers fully in
// memory. The wf_inbox_seq counter is keyed by instance ID (it carries no
// instance_id field), so it is removed by ref; deleting a missing ref is a
// no-op.
func (b *Backend) purgeInstanceDocs(ctx context.Context, id string) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
		batch := b.client.Batch()
		n := 0
		it := b.col(col).Where("instance_id", "==", id).Documents(ctx)
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
			if n == 500 {
				if _, err := batch.Commit(ctx); err != nil {
					it.Stop()
					return err
				}
				batch = b.client.Batch()
				n = 0
			}
		}
		it.Stop()
		if n > 0 {
			if _, err := batch.Commit(ctx); err != nil {
				return err
			}
		}
	}
	_, err := b.ref("wf_inbox_seq", id).Delete(ctx)
	return err
}
