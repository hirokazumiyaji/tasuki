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
// legacy incarnation marker: wf_instances.created_at is written once by
// CreateInstance and never updated, so a row carrying a different value is a
// replacement created after this purge's victim was deleted. incarnation is
// the unique per-incarnation token (see newIncarnation), NULL on legacy
// instance rows that predate the column. Terminal sweeps reuse the same
// carrier with the incarnation captured inside the terminal-status commit
// (see sweepTerminateDocs).
type purgeVictim struct {
	id          string
	createdAt   time.Time
	incarnation string
}

// victimMatches reports whether the instance row currently carrying
// curCreatedAt/curIncarnation is still the fenced incarnation (Codex
// round-21 P1 on #296). When both sides carry a token the tokens must match
// exactly: created_at equality alone breaks on clock rollback, VM restore,
// or timestamp precision truncation, any of which can recreate an ID with
// the same created_at and let a stale sweep delete the replacement's rows.
// When either side lacks a token (legacy rows) the check falls back to
// created_at equality with that documented caveat. Pure for unit tests.
func victimMatches(v purgeVictim, curCreatedAt time.Time, curIncarnation string) bool {
	if v.incarnation != "" && curIncarnation != "" {
		return v.incarnation == curIncarnation
	}
	return curCreatedAt.Equal(v.createdAt)
}

// purgeMarkerTTL bounds a purge marker's life once a replacement incarnation
// exists. Markers are only metadata: with a live replacement the child rows
// stay ambiguous (old stragglers vs the replacement's own) and are left for
// the replacement's own purge either way — the TTL only drops the marker row
// itself so it never outlives its usefulness. Markers with no replacement
// (instance row still absent) resume regardless of age: no new rows could
// have appeared, so every remaining row is provably old.
const purgeMarkerTTL = 7 * 24 * time.Hour

// purgeMarker is the durable incarnation fence for one purge victim: the
// victim ID plus the created_at incarnation observed at listing time and the
// time the victim delete committed. It is written in the SAME transaction as
// the victim delete (see deletePurgedInstanceRow) and cleared only after the
// second sweep and residual reap complete, so a crash in between stays
// recoverable: later purges consult stale markers (see
// resumeStalePurgeMarkers) where the in-memory residual snapshot is lost.
//
// The row key is the victim ID, so a later purge of a replacement
// incarnation overwrites the marker with its own incarnation instead of
// colliding (collision-safe by key; InsertOrUpdate, never bare Insert).
type purgeMarker struct {
	id          string
	createdAt   time.Time
	purgedAt    time.Time
	incarnation string
}

// purgeMarkerMutation builds the marker upsert for a victim delete. Pure
// save for the timestamp for unit tests (see purgeMarkerExpired).
func purgeMarkerMutation(v purgeVictim, now time.Time) *spanner.Mutation {
	return spanner.InsertOrUpdateMap("wf_purge_markers", map[string]any{
		"instance_id":     v.id,
		"created_at":      v.createdAt,
		"purged_at":       now,
		incarnationColumn: v.incarnation,
	})
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
		SQL: `SELECT id, created_at, incarnation FROM wf_instances
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
		var incarnation spanner.NullString
		if err := row.Columns(&v.id, &v.createdAt, &incarnation); err != nil {
			iter.Stop()
			return 0, err
		}
		if incarnation.Valid {
			v.incarnation = incarnation.StringVal
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
	// Crash recovery: each victim delete commits its purge marker atomically,
	// but the trailing sweep/reap runs after — a crash in between leaves
	// orphaned rows no victim listing can rediscover (the instance row is
	// gone). Stale markers resume that cleanup (see
	// resumeStalePurgeMarkers); marker resumes complete cleanups, never new
	// victim deletes, so they are not counted in purged.
	if err := b.resumeStalePurgeMarkers(ctx, nowUTC()); err != nil {
		return purged, err
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
	deleted, residual, purgedAt, err := b.deletePurgedInstanceRow(ctx, v)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	// The marker version this purge just committed: the conditional clear
	// below removes exactly it, never a newer incarnation's marker.
	ownMarker := purgeMarker{id: v.id, createdAt: v.createdAt, purgedAt: purgedAt, incarnation: v.incarnation}
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
// The delete is conditioned on the marker row still carrying the victim's
// created_at AND purged_at (read-check-delete in one transaction): a purge
// that finishes a page, loses the ID to a replacement, and watches another
// purge delete the replacement and overwrite the marker must not delete the
// newer incarnation's marker. A mismatch leaves the row alone (its own purge
// owns it now); a missing row is success.
//
// The victim rows are already gone, so a crash after this point needs no
// recovery; a failure here surfaces (leaving the marker) instead of
// reporting success while a resume stays pending. A later purge then finds
// an empty victim (no rows, no instance row) and clears the marker
// idempotently. Note the purge count edge: the victim was fully purged but
// a marker-clear failure returns before it is counted, so the count misses
// one instance across the failing call and its healing resume (which never
// counts resumes).
func (b *Backend) clearPurgeMarker(ctx context.Context, marker purgeMarker) error {
	return b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "wf_purge_markers", spanner.Key{marker.id},
			[]string{"instance_id", "created_at", "purged_at", "incarnation"})
		if isNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		var cur purgeMarker
		var curIncarnation spanner.NullString
		if err := row.Columns(&cur.id, &cur.createdAt, &cur.purgedAt, &curIncarnation); err != nil {
			return err
		}
		if curIncarnation.Valid {
			cur.incarnation = curIncarnation.StringVal
		}
		if cur.id == "" || !cur.createdAt.Equal(marker.createdAt) || !cur.purgedAt.Equal(marker.purgedAt) ||
			(cur.incarnation != "" && marker.incarnation != "" && cur.incarnation != marker.incarnation) {
			// A newer incarnation's purge overwrote the marker after this
			// purge's victim delete: leave it for its own purge.
			return nil
		}
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_purge_markers", spanner.Key{marker.id}),
		})
	})
}

// resumeStalePurgeMarkers completes victim cleanups whose process crashed
// between the victim-delete commit (which durably writes the purge marker)
// and the trailing sweep/reap (which clears it). The in-memory residual
// snapshot is lost with the crash, so the resume cannot reap by exact
// key — instead it exploits the absent-instance invariant: while the
// instance row stays gone no new child rows can appear (inbox writers read
// wf_instances in-txn and abort into ErrNotFound; CreateInstance would
// recreate the row and trip the fence), so every remaining row is provably
// old and the fenced second sweep removes it safely. If a replacement
// appeared, rows stay ambiguous and are left for the replacement's own
// purge (leak-safe); only the marker itself ages out via purgeMarkerTTL.
func (b *Backend) resumeStalePurgeMarkers(ctx context.Context, now time.Time) error {
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT instance_id, created_at, purged_at, incarnation FROM wf_purge_markers`,
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
		var marker purgeMarker
		var incarnation spanner.NullString
		if err := row.Columns(&marker.id, &marker.createdAt, &marker.purgedAt, &incarnation); err != nil {
			return err
		}
		if incarnation.Valid {
			marker.incarnation = incarnation.StringVal
		}
		if marker.id == "" {
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
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{marker.id}, []string{"created_at", "incarnation"})
	if err != nil && !isNotFound(err) {
		return err
	}
	if err == nil {
		var createdAt time.Time
		var incarnation spanner.NullString
		if err := row.Columns(&createdAt, &incarnation); err != nil {
			return err
		}
		var curIncarnation string
		if incarnation.Valid {
			curIncarnation = incarnation.StringVal
		}
		if victimMatches(purgeVictim{id: marker.id, createdAt: marker.createdAt, incarnation: marker.incarnation}, createdAt, curIncarnation) {
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
	gone := func(ctx context.Context) error { return b.checkPurgeAbsent(ctx, marker.id) }
	goneTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeAbsentTx(ctx, txn, marker.id)
	}
	if err := b.deleteInstanceChildren(ctx, marker.id, gone, goneTx); err != nil {
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

// checkPurgeVictim enforces the first-sweep fence: the victim row must still
// exist with the incarnation observed at listing time. A missing row means a
// concurrent purge already deleted it; a different created_at means the ID
// was recreated after such a delete. Either way the sweep must stop before it
// touches another incarnation's rows.
func (b *Backend) checkPurgeVictim(ctx context.Context, v purgeVictim) error {
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at", "incarnation"})
	if isNotFound(err) {
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	var createdAt time.Time
	var incarnation spanner.NullString
	if err := row.Columns(&createdAt, &incarnation); err != nil {
		return err
	}
	var curIncarnation string
	if incarnation.Valid {
		curIncarnation = incarnation.StringVal
	}
	if !victimMatches(v, createdAt, curIncarnation) {
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
	row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at", "incarnation"})
	if isNotFound(err) {
		return errPurgeSuperseded
	}
	if err != nil {
		return err
	}
	var createdAt time.Time
	var incarnation spanner.NullString
	if err := row.Columns(&createdAt, &incarnation); err != nil {
		return err
	}
	var curIncarnation string
	if incarnation.Valid {
		curIncarnation = incarnation.StringVal
	}
	if !victimMatches(v, createdAt, curIncarnation) {
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
//
// The same transaction durably writes the purge marker (see purgeMarker):
// the in-memory residual above is lost on a crash before the second
// sweep/reap, but the marker lets a later purge rediscover the absent victim
// and resume. The marker upsert overwrites any stale marker from an earlier
// incarnation's crashed purge — keyed by victim ID, so the latest
// incarnation always wins.
func (b *Backend) deletePurgedInstanceRow(ctx context.Context, v purgeVictim) (bool, *residualStragglers, time.Time, error) {
	deleted := false
	var residual residualStragglers
	var purgedAt time.Time
	err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Reset per attempt: the transaction function may run more than
		// once, and only the committing attempt's snapshot classifies the
		// residual reap.
		deleted = false
		residual = residualStragglers{}
		row, err := txn.ReadRow(ctx, "wf_instances", spanner.Key{v.id}, []string{"created_at", "incarnation"})
		if isNotFound(err) {
			return errPurgeSuperseded
		}
		if err != nil {
			return err
		}
		var createdAt time.Time
		var incarnation spanner.NullString
		if err := row.Columns(&createdAt, &incarnation); err != nil {
			return err
		}
		var curIncarnation string
		if incarnation.Valid {
			curIncarnation = incarnation.StringVal
		}
		if !victimMatches(v, createdAt, curIncarnation) {
			return errPurgeSuperseded
		}
		if err := queryResidualDedupeTx(ctx, txn, v.id, &residual); err != nil {
			return err
		}
		if err := queryResidualInboxTx(ctx, txn, v.id, &residual); err != nil {
			return err
		}
		deleted = true
		// Stamp per attempt: a retried transaction must not commit with a
		// purged_at captured before a conflicting commit. Truncated to
		// microseconds so the committed value round-trips exactly and the
		// conditional clearPurgeMarker below can match it (Spanner
		// TIMESTAMP carries microsecond precision).
		purgedAt = nowUTC().Truncate(time.Microsecond)
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{v.id}),
			spanner.Delete("wf_instances", spanner.Key{v.id}),
			purgeMarkerMutation(v, purgedAt),
		})
	})
	if errors.Is(err, errPurgeSuperseded) {
		return false, nil, time.Time{}, nil
	}
	if err != nil {
		return false, nil, time.Time{}, err
	}
	return deleted, &residual, purgedAt, nil
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
	// Post-terminal retry markers are children of the instance like dedupe
	// keys (see postTerminalMarkersTable): without this sweep a purged
	// instance's markers would survive and suppress the next incarnation's
	// sends under the same DedupeIDs.
	if err := b.deletePostTerminalMarkers(ctx, id, guard, guardTx); err != nil {
		return err
	}
	if err := b.deleteInboxForInstance(ctx, id, guard, guardTx); err != nil {
		return err
	}
	return b.deleteJournalForInstance(ctx, id, guard, guardTx)
}

// deletePostTerminalMarkers removes one instance's retry-marker rows in
// guarded pages, mirroring deleteAllSignalDedupe.
func (b *Backend) deletePostTerminalMarkers(ctx context.Context, id string, guard sweepGuard, guardTx sweepGuardTx) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var keys []string
		err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if err := checkGuardTx(ctx, txn, guardTx); err != nil {
				return err
			}
			keys = keys[:0]
			iter := txn.Query(ctx, spanner.Statement{
				SQL:    `SELECT marker_key FROM wf_post_terminal_markers WHERE instance_id = @id LIMIT @limit`,
				Params: map[string]any{"id": id, "limit": int64(spannerSweepBatchSize)},
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
				var k string
				if err := row.Columns(&k); err != nil {
					return err
				}
				keys = append(keys, k)
			}
			if len(keys) == 0 {
				return nil
			}
			var muts []*spanner.Mutation
			for _, k := range keys {
				muts = append(muts, spanner.Delete(postTerminalMarkersTable, spanner.Key{id, k}))
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) < spannerSweepBatchSize {
			return nil
		}
	}
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
