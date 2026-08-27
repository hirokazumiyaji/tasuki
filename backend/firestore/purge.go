package firestore

import (
	"context"
	"time"

	gcf "cloud.google.com/go/firestore"
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
// together with their tasks, timers, dedupe entries, inbox items and journal.
// Documents are removed in batches; concurrent progress on the same instance
// is not possible (terminal instances are immutable), so best-effort batching
// is safe.
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
		if _, err := b.ref("wf_instances", id).Delete(ctx); err != nil {
			return purged, err
		}
		purged++
	}
	return purged, nil
}

func (b *Backend) purgeInstanceDocs(ctx context.Context, id string) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
		var refs []*gcf.DocumentRef
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
			refs = append(refs, dsnap.Ref)
		}
		it.Stop()
		for start := 0; start < len(refs); start += 500 {
			end := min(start+500, len(refs))
			batch := b.client.Batch()
			for _, r := range refs[start:end] {
				batch.Delete(r)
			}
			if _, err := batch.Commit(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
