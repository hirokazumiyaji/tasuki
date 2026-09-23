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

// purgeFence pins one purge victim's ID-reuse fence for every sweep page:
// the victim incarnation (id + createdAt observed at listing) and which
// side of the victim delete the sweep runs on. The first sweep requires the
// victim doc to still carry the listed created_at; the second requires the
// doc to stay gone — any doc present then is a replacement incarnation
// whose documents must never be deleted.
type purgeFence struct {
	victim purgeVictim
	// absent selects the second-sweep fence (victim doc must stay gone);
	// false selects the first-sweep fence (doc must still carry the
	// listed incarnation).
	absent bool
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
// uncounted); the second sweep proceeds only while the doc stays absent. When
// the second sweep stops at a replacement, residual stragglers snapshotted
// before the delete are still reaped by exact reference (see
// listResidualStragglers) instead of leaking into the
// replacement. Stragglers that cannot be proven old leak, and are reaped with
// the replacement's own purge once it is terminal — leaked rows are always
// preferable to deleting a live incarnation's documents.
func (b *Backend) purgeOneInstance(ctx context.Context, v purgeVictim) (bool, error) {
	first := purgeFence{victim: v}
	if err := b.purgeInstanceDocs(ctx, first); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			return false, nil
		}
		return false, err
	}
	// The residual snapshot is taken inside the victim-delete transaction
	// (deletePurgedInstanceDoc), not by a separate listing: the delete
	// transaction is the serialization point against an ID-reusing
	// CreateInstance (which inserts the replacement only after the victim
	// doc is gone), so any row the snapshot observes provably predates any
	// replacement by transaction order, not by wall-clock comparison.
	// Stragglers that commit concurrently with the delete and serialize
	// after it are excluded from the snapshot and leak safely; they are
	// reaped with the replacement's own purge once it is terminal. Leaked
	// rows are always preferable to deleting a live incarnation's rows.
	//
	// Inbox writers read wf_instances inside their transaction, so once
	// the delete below commits no new child documents can appear for the
	// instance (in-flight writers lose the race and retry into ErrNotFound).
	// A writer that committed between the sweep above and this delete is
	// reaped by the second pass below.
	deleted, residual, err := b.deletePurgedInstanceDoc(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	second := purgeFence{victim: v, absent: true}
	if err := b.purgeInstanceDocs(ctx, second); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			// A replacement incarnation appeared after this purge deleted
			// the victim: an inbox row (or dedupe key) that committed
			// between the first sweep and the delete belongs to the old
			// incarnation, but the fence above leaves it for the
			// replacement's LoadWorkflow to consume. Reap the residual rows
			// snapshotted above by exact reference instead of leaking them.
			// This purge still owns the victim delete, so it stays counted.
			if rerr := b.reapResidualStragglers(ctx, residual); rerr != nil {
				return true, rerr
			}
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

// checkFenceTx enforces a purge fence inside a page-delete (or
// victim-delete) transaction: the victim doc must still carry the listed
// incarnation (first sweep) or stay gone (second sweep). Reading the fence
// inside the transaction shares one snapshot with the page query, so an
// ID-reusing CreateInstance is either invisible to both (only provably-old
// documents are deleted) or visible to both (the fence trips and the
// transaction aborts before deleting anything).
func (b *Backend) checkFenceTx(tx *gcf.Transaction, fence purgeFence) error {
	snap, err := tx.Get(b.ref("wf_instances", fence.victim.id))
	if isNotFound(err) || (err == nil && !snap.Exists()) {
		if fence.absent {
			return nil
		}
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	if fence.absent {
		return errPurgeSuperseded
	}
	if !timestamp(snap.Data(), "created_at").Equal(fence.victim.createdAt) {
		return errPurgeSuperseded
	}
	return nil
}

// deletePurgedInstanceDoc removes the victim doc (plus its inbox-seq counter)
// only if it still carries the listed incarnation. A concurrent purge that
// won the delete, or a replacement created since, yields (false, nil, nil):
// this purge owns nothing and must neither sweep further nor count the
// instance. The counter goes in the same transaction so a replacement created
// right after the delete always starts from a fresh sequence.
//
// On a successful delete it also returns the residual snapshot: the dedupe
// and inbox rows observed inside this same transaction (all reads precede
// the writes, per Firestore transaction rules). The delete transaction is
// the serialization point against an ID-reusing CreateInstance (which
// inserts the replacement only after the victim doc is gone), so every
// snapshotted row provably predates any replacement — atomically, with no
// listing-to-delete gap for a terminal SendToInbox to slip a row into that
// the exact-reap could then mistake for the replacement's. Rows that
// serialize after this transaction are excluded and leak safely for the
// replacement's own purge. Rows are keyed by exact document reference:
// dedupe document IDs are deterministic, so the snapshot also pins each
// dedupe row's server update time and the reap deletes only rows still
// carrying it (see reapResidualStragglers).
func (b *Backend) deletePurgedInstanceDoc(ctx context.Context, v purgeVictim) (bool, *residualStragglers, error) {
	var (
		deleted  bool
		residual residualStragglers
	)
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		// Reset per attempt: the transaction function may run more than
		// once, and only the committing attempt's snapshot classifies the
		// residual reap.
		deleted = false
		residual = residualStragglers{}
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
		if err := queryResidualDedupeTx(tx, b.col("wf_signal_dedupe"), v.id, &residual); err != nil {
			return err
		}
		if err := queryResidualInboxTx(tx, b.col("wf_inbox"), v.id, &residual); err != nil {
			return err
		}
		if err := tx.Delete(b.ref("wf_inbox_seq", v.id)); err != nil {
			return err
		}
		deleted = true
		return tx.Delete(b.ref("wf_instances", v.id))
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return deleted, &residual, nil
}

// residualStragglers holds exact document references observed after the
// first sweep but before the victim delete (see purgeOneInstance). Deleting
// by reference avoids cross-process wall-clock skew: created_at stamps from
// different nodes cannot be compared reliably, but a document listed before
// the delete provably predates any replacement created after it.
//
// Dedupe keys additionally carry the server-stamped update time observed at
// listing: dedupe document IDs are deterministic
// (instanceID + ":" + escaped DedupeID), so a replacement incarnation can
// recreate the very same document between the snapshot and the reap. The
// reap deletes a dedupe document only when its update time still matches the
// snapshot (dedupe rows are create-once, never updated, so any difference
// proves the row is the replacement's, not the straggler's). Inbox documents
// use random IDs a replacement cannot reuse, so exact-reference deletes stay
// unconditional for them.
type residualStragglers struct {
	dedupe []dedupeVersion
	inbox  []*gcf.DocumentRef
}

// dedupeVersion pins one snapshotted dedupe row: its exact reference plus
// the server update time at snapshot time (see residualStragglers).
type dedupeVersion struct {
	ref        *gcf.DocumentRef
	updateTime time.Time
}

// listResidualStragglers snapshots the dedupe and inbox rows that survived
// the first sweep (stragglers committed during the sweep). Only these two
// collections can gain rows while the victim is terminal — SendToInbox
// inserts both and wakes nothing once the instance is terminal. Task, timer
// and journal rows are written only on behalf of the running victim.
// Production purge takes this snapshot inside the victim-delete transaction
// instead (see deletePurgedInstanceDoc); this standalone listing serves
// tests that seed rows directly.
func (b *Backend) listResidualStragglers(ctx context.Context, id string) (*residualStragglers, error) {
	var out residualStragglers
	it := b.col("wf_signal_dedupe").Where("instance_id", "==", id).Documents(ctx)
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			it.Stop()
			return nil, err
		}
		out.dedupe = append(out.dedupe, dedupeVersion{ref: dsnap.Ref, updateTime: dsnap.UpdateTime})
	}
	it.Stop()
	ibit := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
	for {
		dsnap, err := ibit.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			ibit.Stop()
			return nil, err
		}
		out.inbox = append(out.inbox, dsnap.Ref)
	}
	ibit.Stop()
	return &out, nil
}

// queryResidualDedupeTx reads the victim's dedupe rows inside the
// victim-delete transaction for the atomic residual snapshot (see
// deletePurgedInstanceDoc).
func queryResidualDedupeTx(tx *gcf.Transaction, col *gcf.CollectionRef, id string, out *residualStragglers) error {
	it := tx.Documents(col.Where("instance_id", "==", id))
	defer it.Stop()
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		out.dedupe = append(out.dedupe, dedupeVersion{ref: dsnap.Ref, updateTime: dsnap.UpdateTime})
	}
}

// queryResidualInboxTx reads the victim's inbox rows inside the
// victim-delete transaction for the atomic residual snapshot (see
// deletePurgedInstanceDoc).
func queryResidualInboxTx(tx *gcf.Transaction, col *gcf.CollectionRef, id string, out *residualStragglers) error {
	it := tx.Documents(col.Where("instance_id", "==", id))
	defer it.Stop()
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		out.inbox = append(out.inbox, dsnap.Ref)
	}
}

// reapResidualStragglers deletes the snapshotted residual rows in paged
// batches. Rows already gone (deleted by the second sweep before it tripped)
// are skipped via best-effort deletes; rows never snapshotted (replacement
// rows and post-snapshot stragglers) are preserved.
//
// Dedupe rows are deleted conditionally: the snapshotted server update time
// must still match, otherwise the row was recreated by a replacement
// incarnation reusing the same DedupeID after the snapshot (Codex round 6 on
// #327) and deleting it would strip the replacement's live dedupe guard
// while its inbox event remains, duplicating a later retry. A straggler that
// the replacement inherited (deduped against, so no inbox event of its own)
// still carries the snapshotted version and is reaped, letting a retry
// insert anew.
//
// Datastore errors from the conditional deletes propagate to the caller:
// only not-found (row already gone) and version-mismatch (row recreated by
// the replacement, skipped above) are benign. A transient transaction
// failure must fail the purge rather than report success while a stale
// dedupe row stays attached — a later SendToInbox for that DedupeID would
// find the stale row and drop the event.
func (b *Backend) reapResidualStragglers(ctx context.Context, r *residualStragglers) error {
	if r == nil {
		return nil
	}
	for _, dv := range r.dedupe {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
			snap, err := tx.Get(dv.ref)
			if isNotFound(err) || (err == nil && !snap.Exists()) {
				return nil
			}
			if err != nil {
				return err
			}
			if !snap.UpdateTime.Equal(dv.updateTime) {
				return nil
			}
			return tx.Delete(dv.ref)
		}); err != nil {
			return err
		}
	}
	for start := 0; start < len(r.inbox); start += firestoreSweepBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + firestoreSweepBatchSize
		if end > len(r.inbox) {
			end = len(r.inbox)
		}
		//lint:ignore SA1019 WriteBatch still functional; migrate to BulkWriter in an emulator-verified follow-up.
		batch := b.client.Batch()
		for _, ref := range r.inbox[start:end] {
			batch.Delete(ref)
		}
		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// purgeInstanceDocs removes every child document of one instance in
// transactionally fenced pages (see deleteDocsByInstanceFenced). Batches
// commit while iterating so a large journal or inbox never buffers fully in
// memory. The fence holds the ID-reuse fence through every page: a tripped
// fence aborts the sweep so a replacement incarnation's documents are never
// deleted.
func (b *Backend) purgeInstanceDocs(ctx context.Context, fence purgeFence) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal"} {
		if err := b.deleteDocsByInstanceFenced(ctx, col, fence); err != nil {
			return err
		}
	}
	return nil
}
