package firestore

import (
	"context"
	"errors"
	"time"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

// errPurgeSuperseded aborts a purge that no longer owns its victim: a
// concurrent purge already deleted the instance doc, or the ID was recreated
// after the delete. It is translated to an uncounted (or, after this purge's
// own delete, counted) nil return — never to a caller-visible error.
var errPurgeSuperseded = errors.New("firestore: purge victim superseded")

// purgeVictim is one instance selected for purging. createdAt is the
// incarnation marker: wf_instances.created_at is written once by
// CreateInstance and never updated, so a doc carrying a different value is a
// replacement created after this purge's victim was deleted.
type purgeVictim struct {
	id        string
	createdAt time.Time
}

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

	var victims []purgeVictim
	it := b.col("wf_instances").Where("completed_at", "<=", cutoff).Documents(ctx)
	defer it.Stop()
	for len(victims) < lim {
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
		victims = append(victims, purgeVictim{id: snap.Ref.ID, createdAt: timestamp(snap.Data(), "created_at")})
	}

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

// purgeOneInstance removes one terminal instance with its child documents and
// reports whether this purge owned the delete. A guarded child sweep goes
// first, then the instance doc (plus its inbox-seq counter) goes in one
// incarnation-checked transaction, then a second guarded child sweep reaps
// writers that committed between the first sweep and the doc delete (inbox
// writers read wf_instances inside their transaction, so once the delete
// commits no new child documents can appear: in-flight writers lose the race
// and retry into ErrNotFound).
//
// Both sweeps hold the ID-reuse fence through every batch: CreateInstance may
// recreate the same ID as soon as the instance doc is gone, and deletes keyed
// only by the reused ID would then corrupt the replacement. The first sweep
// proceeds only while the instance doc still carries the listed created_at;
// the delete commits only for that same incarnation (a concurrent purge that
// deleted first, or a replacement created since, makes this purge stand down
// uncounted); the second sweep proceeds only while the doc stays absent and
// stops at the first sign of a replacement. Stragglers that slip past a
// stopped second sweep leak, and are reaped with the replacement's own purge
// once it is terminal — leaked rows are always preferable to deleting a live
// incarnation's documents.
func (b *Backend) purgeOneInstance(ctx context.Context, v purgeVictim) (bool, error) {
	own := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, v) }
	if err := b.purgeInstanceDocs(ctx, v.id, own); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return false, nil
		}
		return false, err
	}
	// Inbox writers read wf_instances inside their transaction, so once
	// this delete commits no new child documents can appear for the
	// instance (in-flight writers lose the race and retry into
	// ErrNotFound). A writer that committed between the sweep above and
	// this delete is reaped by the second pass below.
	deleted, err := b.deletePurgedInstanceDoc(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	gone := func(ctx context.Context) error { return b.checkPurgeAbsent(ctx, v.id) }
	if err := b.purgeInstanceDocs(ctx, v.id, gone); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// checkPurgeVictim enforces the first-sweep fence: the victim doc must still
// exist with the incarnation observed at listing time. A missing doc means a
// concurrent purge already deleted it; a different created_at means the ID
// was recreated after such a delete. Either way the sweep must stop before it
// touches another incarnation's documents.
func (b *Backend) checkPurgeVictim(ctx context.Context, v purgeVictim) error {
	snap, err := b.ref("wf_instances", v.id).Get(ctx)
	if isNotFound(err) || (err == nil && !snap.Exists()) {
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	if !timestamp(snap.Data(), "created_at").Equal(v.createdAt) {
		return errPurgeSuperseded
	}
	return nil
}

// checkPurgeAbsent enforces the second-sweep fence: the victim doc must stay
// gone. Any doc present now is a replacement incarnation, so the sweep stops
// before deleting its documents.
func (b *Backend) checkPurgeAbsent(ctx context.Context, id string) error {
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if isNotFound(err) || (err == nil && !snap.Exists()) {
		return nil
	}
	if err != nil {
		return err
	}
	return errPurgeSuperseded
}

// deletePurgedInstanceDoc removes the victim doc (plus its inbox-seq counter)
// only if it still carries the listed incarnation. A concurrent purge that
// won the delete, or a replacement created since, yields (false, nil): this
// purge owns nothing and must neither sweep further nor count the instance.
// The counter goes in the same transaction so a replacement created right
// after the delete always starts from a fresh sequence.
func (b *Backend) deletePurgedInstanceDoc(ctx context.Context, v purgeVictim) (bool, error) {
	deleted := false
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		snap, err := tx.Get(b.ref("wf_instances", v.id))
		if isNotFound(err) || (err == nil && !snap.Exists()) {
			return errPurgeSuperseded
		}
		if err != nil {
			return err
		}
		if !timestamp(snap.Data(), "created_at").Equal(v.createdAt) {
			return errPurgeSuperseded
		}
		if err := tx.Delete(b.ref("wf_inbox_seq", v.id)); err != nil {
			return err
		}
		deleted = true
		return tx.Delete(b.ref("wf_instances", v.id))
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// purgeInstanceDocs removes every child document of one instance. Batches
// commit while iterating so a large journal or inbox never buffers fully in
// memory. The guard (nil for no fence) holds the ID-reuse fence: it is
// re-checked before each collection and after every batch commit, so the
// sweep stops as soon as the victim incarnation is gone or replaced instead
// of deleting a live replacement's documents.
func (b *Backend) purgeInstanceDocs(ctx context.Context, id string, guard sweepGuard) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
		if guard != nil {
			if err := guard(ctx); err != nil {
				return err
			}
		}
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
				if guard != nil {
					if err := guard(ctx); err != nil {
						it.Stop()
						return err
					}
				}
			}
		}
		it.Stop()
		if n > 0 {
			if _, err := batch.Commit(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
