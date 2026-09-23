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

// purgeMarkerTTL bounds a purge marker's life once a replacement incarnation
// exists. Markers are only metadata: with a live replacement the child rows
// stay ambiguous (old stragglers vs the replacement's own) and are left for
// the replacement's own purge either way — the TTL only drops the marker doc
// itself so it never outlives its usefulness. Markers with no replacement
// (instance doc still absent) resume regardless of age: no new rows could
// have appeared, so every remaining row is provably old.
const purgeMarkerTTL = 7 * 24 * time.Hour

// purgeMarkersCollection holds one document per purge victim whose instance
// doc is already gone but whose trailing sweep/reap may not have finished.
const purgeMarkersCollection = "wf_purge_markers"

// purgeMarker is the durable incarnation fence for one purge victim: the
// victim ID plus the created_at incarnation observed at listing time and the
// time the victim delete committed. It is written in the SAME transaction as
// the victim delete (see deletePurgedInstanceDoc) and cleared only after the
// second sweep and residual reap complete, so a crash in between stays
// recoverable: later purges consult stale markers (see
// resumeStalePurgeMarkers) where the in-memory residual snapshot is lost.
//
// The document ID is the victim ID, so a later purge of a replacement
// incarnation overwrites the marker with its own incarnation instead of
// colliding (collision-safe by key).
type purgeMarker struct {
	id        string
	createdAt time.Time
	purgedAt  time.Time
}

// purgeMarkerDoc builds the marker document for a victim delete. Pure for
// unit tests.
func purgeMarkerDoc(v purgeVictim, now time.Time) map[string]any {
	return map[string]any{
		"instance_id": v.id,
		"created_at":  v.createdAt,
		"purged_at":   now,
	}
}

// decodePurgeMarker reads a marker document. ok=false when the document
// carries no usable victim identity.
func decodePurgeMarker(snapID string, m map[string]any) (purgeMarker, bool) {
	id := str(m, "instance_id")
	if id == "" {
		id = snapID
	}
	if id == "" {
		return purgeMarker{}, false
	}
	return purgeMarker{id: id, createdAt: timestamp(m, "created_at"), purgedAt: timestamp(m, "purged_at")}, true
}

// purgeMarkerExpired reports whether a marker with a live replacement may be
// dropped (see purgeMarkerTTL). Markers without a write timestamp never
// expire; they linger until the replacement's own purge overwrites them.
func purgeMarkerExpired(now, purgedAt time.Time) bool {
	if purgedAt.IsZero() {
		return false
	}
	return now.Sub(purgedAt) >= purgeMarkerTTL
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
	// Crash recovery: each victim delete commits its purge marker atomically,
	// but the trailing sweep/reap runs after — a crash in between leaves
	// orphaned rows no victim listing can rediscover (the instance doc is
	// gone). Stale markers resume that cleanup (see
	// resumeStalePurgeMarkers); marker resumes complete cleanups, never new
	// victim deletes, so they are not counted in purged.
	if err := b.resumeStalePurgeMarkers(ctx, nowUTC()); err != nil {
		return purged, err
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
	deleted, residual, purgedAt, err := b.deletePurgedInstanceDoc(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	// The marker version this purge just committed: the conditional clear
	// below removes exactly it, never a newer incarnation's marker.
	ownMarker := purgeMarker{id: v.id, createdAt: v.createdAt, purgedAt: purgedAt}
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
			// The residual reap is the last step owning victim rows: only
			// now is the durable marker cleared. A crash before this point
			// resumes via resumeStalePurgeMarkers.
			if cerr := b.clearPurgeMarker(ctx, ownMarker); cerr != nil {
				return true, cerr
			}
			return true, nil
		}
		return false, err
	}
	// The second sweep is the last step owning victim rows: only now is the
	// durable marker cleared (see above for the crash window this closes).
	if err := b.clearPurgeMarker(ctx, ownMarker); err != nil {
		return true, err
	}
	return true, nil
}

// clearPurgeMarker removes a victim's purge marker after its trailing
// sweep/reap completed — but only the marker for the completed incarnation.
// The delete is conditioned on the marker still carrying the victim's
// created_at AND purged_at (read-check-delete in one transaction): a purge
// that finishes a page, loses the ID to a replacement, and watches another
// purge delete the replacement and overwrite the marker must not delete the
// newer incarnation's marker. A mismatch leaves the row alone (its own purge
// owns it now); a missing row is success.
//
// The victim rows are already gone, so a crash after this point needs no
// recovery; a failure here surfaces (leaving the marker) instead of
// reporting success while a resume stays pending. A later purge then finds
// an empty victim (no rows, no instance doc) and clears the marker
// idempotently. Note the purge count edge: the victim was fully purged but
// a marker-clear failure returns before it is counted, so the count misses
// one instance across the failing call and its healing resume (which never
// counts resumes).
func (b *Backend) clearPurgeMarker(ctx context.Context, marker purgeMarker) error {
	return b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		snap, err := tx.Get(b.ref(purgeMarkersCollection, marker.id))
		if isNotFound(err) || (err == nil && !snap.Exists()) {
			return nil
		}
		if err != nil {
			return err
		}
		cur, ok := decodePurgeMarker(snap.Ref.ID, snap.Data())
		if !ok || !cur.createdAt.Equal(marker.createdAt) || !cur.purgedAt.Equal(marker.purgedAt) {
			// A newer incarnation's purge overwrote the marker after this
			// purge's victim delete: leave it for its own purge.
			return nil
		}
		return tx.Delete(snap.Ref)
	})
}

// resumeStalePurgeMarkers completes victim cleanups whose process crashed
// between the victim-delete commit (which durably writes the purge marker)
// and the trailing sweep/reap (which clears it). The in-memory residual
// snapshot is lost with the crash, so the resume cannot reap by exact
// reference — instead it exploits the absent-instance invariant: while the
// instance doc stays gone no new child documents can appear (inbox writers
// read wf_instances in-txn and abort into ErrNotFound; CreateInstance would
// recreate the doc and trip the fence), so every remaining row is provably
// old and the fenced second sweep removes it safely. If a replacement
// appeared, rows stay ambiguous and are left for the replacement's own
// purge (leak-safe); only the marker itself ages out via purgeMarkerTTL.
func (b *Backend) resumeStalePurgeMarkers(ctx context.Context, now time.Time) error {
	it := b.col(purgeMarkersCollection).Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		marker, ok := decodePurgeMarker(snap.Ref.ID, snap.Data())
		if !ok {
			continue
		}
		if err := b.resumeOnePurgeMarker(ctx, marker, now); err != nil {
			return err
		}
	}
}

// resumeOnePurgeMarker resumes a single stale marker (see
// resumeStalePurgeMarkers). A resumed cleanup that finds nothing left still
// clears the marker; a resume racing a fresh replacement trips the absent
// fence and keeps the marker for a later pass.
func (b *Backend) resumeOnePurgeMarker(ctx context.Context, marker purgeMarker, now time.Time) error {
	snap, err := b.ref("wf_instances", marker.id).Get(ctx)
	if err != nil && !isNotFound(err) {
		return err
	}
	if err == nil && snap.Exists() {
		if timestamp(snap.Data(), "created_at").Equal(marker.createdAt) {
			// Same incarnation present: the delete transaction writes the
			// marker and the delete atomically, so this is unreachable
			// barring manual writes. Leave the marker: a later purge of
			// this victim overwrites it in its own delete transaction.
			return nil
		}
		// Replacement incarnation: rows are ambiguous, so they stay for the
		// replacement's own purge. Drop only the marker itself once stale.
		if purgeMarkerExpired(now, marker.purgedAt) {
			return b.clearPurgeMarker(ctx, marker)
		}
		return nil
	}
	fence := purgeFence{victim: purgeVictim{id: marker.id, createdAt: marker.createdAt}, absent: true}
	if err := b.purgeInstanceDocs(ctx, fence); err != nil {
		if errors.Is(err, errPurgeSuperseded) {
			// A replacement appeared mid-resume: rows are ambiguous now.
			// Keep the marker; a later pass (or the replacement's own
			// purge, which overwrites it) retries.
			return nil
		}
		return err
	}
	return b.clearPurgeMarker(ctx, marker)
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
//
// The same transaction durably writes the purge marker (see purgeMarker):
// the in-memory residual above is lost on a crash before the second
// sweep/reap, but the marker lets a later purge rediscover the absent victim
// and resume. The marker Set overwrites any stale marker from an earlier
// incarnation's crashed purge — keyed by victim ID, so the latest
// incarnation always wins.
func (b *Backend) deletePurgedInstanceDoc(ctx context.Context, v purgeVictim) (bool, *residualStragglers, time.Time, error) {
	var (
		deleted  bool
		residual residualStragglers
		purgedAt time.Time
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
		// Stamp per attempt (see SendToInboxBatch): a retried transaction
		// must not commit with a purged_at captured before a conflicting
		// commit. All reads precede this first write. Truncated to
		// microseconds so the committed value round-trips exactly and the
		// conditional clearPurgeMarker below can match it (Firestore
		// timestamps carry microsecond precision).
		purgedAt = nowUTC().Truncate(time.Microsecond)
		if err := tx.Set(b.ref(purgeMarkersCollection, v.id), purgeMarkerDoc(v, purgedAt)); err != nil {
			return err
		}
		if err := tx.Delete(b.ref("wf_inbox_seq", v.id)); err != nil {
			return err
		}
		deleted = true
		return tx.Delete(b.ref("wf_instances", v.id))
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil, time.Time{}, nil
	}
	if err != nil {
		return false, nil, time.Time{}, err
	}
	return deleted, &residual, purgedAt, nil
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
// deleted. Post-terminal retry markers live in their own collection (see
// postTerminalMarkersCollection) and are swept here like any other child:
// without this, a purged instance's markers would survive and suppress the
// next incarnation's sends under the same DedupeIDs.
func (b *Backend) purgeInstanceDocs(ctx context.Context, fence purgeFence) error {
	for _, col := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox", "wf_journal", postTerminalMarkersCollection} {
		if err := b.deleteDocsByInstanceFenced(ctx, col, fence); err != nil {
			return err
		}
	}
	return nil
}
