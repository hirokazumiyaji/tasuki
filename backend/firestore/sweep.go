package firestore

import (
	"context"
	"errors"
	"time"

	gcf "cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// Firestore caps a transaction (and a batched write) at 500 operations.
// Terminal paths must never scale one commit with the instance's accumulated
// rows, so sweeps page deletes well below the cap.
const (
	firestoreTxWriteLimit   = 500
	firestoreSweepBatchSize = 400
)

// signalDedupeSweepTimeout bounds the best-effort post-commit dedupe sweep in
// CommitAdvancements. The sweep runs synchronously so a redelivered DedupeID
// inserts anew once the call returns, but a degraded store must not hold the
// caller (or a worker slot during shutdown) behind unbounded retries: on
// timeout the leftovers stay for purge and terminal notification has already
// fired (it is emitted before the sweep).
const signalDedupeSweepTimeout = 30 * time.Second

// batchesNeeded reports how many sweep batches cover total rows at the given
// batch size. It documents the chunking math behind the paged sweeps below
// and is exercised by unit tests (600 dedupe rows must never fit one commit).
func batchesNeeded(total, size int) int {
	if total <= 0 {
		return 0
	}
	if size <= 0 {
		return total
	}
	return (total + size - 1) / size
}

// chunkStrings splits keys into consecutive batches of at most size. Sweep
// loops page the store with Limit queries instead of buffering every key, but
// this helper captures the same chunking contract for unit tests and for any
// future collect-then-delete path.
func chunkStrings(in []string, size int) [][]string {
	if size <= 0 {
		size = len(in)
	}
	if len(in) == 0 || size <= 0 {
		return nil
	}
	var out [][]string
	for len(in) > 0 {
		n := size
		if n > len(in) {
			n = len(in)
		}
		out = append(out, in[:n])
		in = in[n:]
	}
	return out
}

// sweepTerminateDocs removes the mutable child documents of a terminated
// instance in transactionally fenced pages. The status flip already
// committed, so each page is an independent commit — but every page still
// re-validates the terminal fence (see below): a PurgeInstances that deletes
// the terminal instance, clears its marker, and lets CreateInstance reuse
// the ID while this sweep is paused must stop the resumed sweep before it
// deletes the replacement's documents (Codex round 20 on #296). The fence
// pins the pre-commit incarnation (token plus created_at captured inside
// the flip transaction); on mismatch the sweep aborts with a nil return and
// purge owns the leftovers — leaked rows are always preferable to deleting
// a live incarnation's documents. Dedupe cleanup deletes only the pre-termination
// non-marker keys snapshotted inside the flip transaction: post-terminal
// sends committing after the flip (marker plus inbox event) are absent from
// the snapshot and survive, and markers present in the snapshot itself are
// filtered out (Codex round 8 on #327: sweeping a marker while its inbox
// event remains duplicates the next retry). Purge reaps leftovers.
func (b *Backend) sweepTerminateDocs(ctx context.Context, victim purgeVictim, dedupeSnapshot []string) error {
	// The present-incarnation fence (see purgeFence): the sweep proceeds
	// only while the instance doc still carries the incarnation captured at
	// the terminal commit. A missing doc (purge deleted it) or a different
	// incarnation (the ID was recreated after such a delete) aborts the
	// sweep before it touches another incarnation's documents. The token
	// comparison (see victimMatches) survives clock rollback, VM restore,
	// and timestamp truncation that can all reproduce the same created_at.
	fence := purgeFence{victim: victim}
	id := victim.id
	for _, col := range []string{"wf_tasks", "wf_timers"} {
		if err := b.deleteDocsByInstanceFenced(ctx, col, fence); err != nil {
			if errors.Is(err, errPurgeSuperseded) {
				return nil
			}
			return err
		}
	}
	if err := b.sweepSignalDedupeIDs(ctx, fence, filterTerminateDedupeDocs(dedupeSnapshot, id)); err != nil {
		return err
	}
	// Inbox rows predating the flip are swept by server commit order (see
	// cleanupTerminalDocs): a post-terminal send's event survives with its
	// marker. A replacement incarnation trips the fence and owns its rows.
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if isNotFound(err) || (err == nil && (!snap.Exists() || !victimMatches(victim, timestamp(snap.Data(), "created_at"), str(snap.Data(), incarnationField)))) {
		return nil
	}
	if err != nil {
		return err
	}
	return b.deleteTerminalColDocs(ctx, "wf_inbox", id, snap.UpdateTime)
}

// filterTerminateDedupeDocs keeps only the pre-termination non-marker keys of
// a terminate snapshot (Codex round 8 on #327). Pure for unit tests.
func filterTerminateDedupeDocs(docIDs []string, instanceID string) []string {
	var out []string
	for _, docID := range docIDs {
		if !isPostTerminalMarkerDocID(docID, instanceID) {
			out = append(out, docID)
		}
	}
	return out
}

// listSignalDedupeIDs returns the document IDs of every dedupe key
// currently stored for one instance. It serves tests that seed keys
// directly; CommitAdvancements snapshots inside its commit transaction
// instead (see listSignalDedupeIDsTx), and the post-commit sweep then deletes
// exactly those IDs (see sweepSignalDedupeIDs).
func (b *Backend) listSignalDedupeIDs(ctx context.Context, id string) ([]string, error) {
	it := b.col("wf_signal_dedupe").Where("instance_id", "==", id).Documents(ctx)
	var out []string
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			it.Stop()
			return nil, err
		}
		out = append(out, dsnap.Ref.ID)
	}
	it.Stop()
	return out, nil
}

// listSignalDedupeIDsTx reads the same key set inside a Firestore transaction
// (read phase only: the caller must buffer no writes before calling it).
// CommitAdvancements uses this so the snapshot is a serializable read: a
// SendToInbox serializing before the terminal commit is included in the
// post-commit sweep instead of lingering until purge.
func listSignalDedupeIDsTx(tx *gcf.Transaction, col *gcf.CollectionRef, id string) ([]string, error) {
	it := tx.Documents(col.Where("instance_id", "==", id))
	var out []string
	for {
		dsnap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			it.Stop()
			return nil, err
		}
		out = append(out, dsnap.Ref.ID)
	}
	it.Stop()
	return out, nil
}

// sweepSignalDedupeIDs removes exactly the given dedupe documents in
// transactionally fenced pages. Called best-effort after terminal
// advancements commit with the in-transaction snapshot from
// listSignalDedupeIDsTx.
//
// Keys are classified by transaction serialization order, not client
// timestamps: a SendToInbox that captures created_at before the terminal
// commit, loses the race, and retry-commits after it would otherwise stamp
// a pre-commit time and be swept wrongly (deleting its key while the inbox
// event remains, so a later retry duplicates the signal). Snapshot IDs can
// never match such keys: only IDs visible to the terminal commit are
// removed, and keys first appearing after are preserved even when their
// client timestamp predates the commit.
//
// One residual edge (safe direction: cleanup is delayed, never wrongful): a
// send committing after the snapshot read but before the terminal commit
// lands is preserved for purge even though it logically predates the commit.
//
// Every page runs in one transaction that first re-validates the terminal
// fence (the instance doc must still carry the pre-commit incarnation) and
// then deletes the page: dedupe document IDs are deterministic
// (instanceID + escaped DedupeID), so a replacement incarnation reusing the
// ID after a purge can recreate the very same document. An unfenced
// exact-key delete would then strip the replacement's live guard while its
// inbox event remains, duplicating a later retry (Codex round 20 on #296).
// The fence read and the deletes share the transaction's snapshot, so a
// recreation is either invisible to both (only provably-old rows are
// deleted) or visible to both (the fence trips and the page aborts). On a
// tripped fence the sweep aborts with a nil return: purge owns the
// leftovers.
//
// The sweep stays synchronous so a redelivered DedupeID inserts anew once
// CommitAdvancements returns, but runs under a bounded context so a stuck
// store delays only this cleanup, never the caller. Purge reaps leftovers.
func (b *Backend) sweepSignalDedupeIDs(ctx context.Context, fence purgeFence, ids []string) error {
	for _, chunk := range chunkStrings(ids, firestoreSweepBatchSize) {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := chunk
		err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
			if err := b.checkFenceTx(tx, fence); err != nil {
				return err
			}
			// All reads precede all writes (Firestore transaction rule):
			// the fence read above is the only read; the deletes follow.
			for _, docID := range chunk {
				if err := tx.Delete(b.ref("wf_signal_dedupe", docID)); err != nil {
					return err
				}
			}
			return nil
		})
		if errors.Is(err, errPurgeSuperseded) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteDocsByInstanceFenced deletes every document in col with
// instance_id == id in transactionally fenced pages: each page runs in one
// Firestore transaction that first re-validates the incarnation fence
// against the instance doc and then reads and deletes the page. The fence
// read and the page query share the transaction's snapshot, so a
// CreateInstance that recreates the ID is either invisible to both (only
// provably-old rows are deleted) or visible to both (the fence trips and
// the page aborts before deleting anything). A standalone check outside the
// delete would leave a gap where the recreation commits between check and
// delete, and the delete would then corrupt the replacement — hence one
// transaction per page. Purge sweeps arm the fence with their listed victim
// (see purgeInstanceDocs); terminal sweeps arm it with the pre-commit
// incarnation (see sweepTerminateDocs).
//
// The transaction function is idempotent (fence re-read, page re-query,
// same deletes) so Firestore's internal retries on contention are safe.
// errPurgeSuperseded aborts the sweep: a replacement incarnation appeared
// and its documents must never be touched.
func (b *Backend) deleteDocsByInstanceFenced(ctx context.Context, col string, fence purgeFence) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := 0
		err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
			if err := b.checkFenceTx(tx, fence); err != nil {
				return err
			}
			// All reads precede all writes (Firestore transaction rule):
			// collect the page's references first, then delete them.
			it := tx.Documents(b.col(col).Where("instance_id", "==", fence.victim.id).Limit(firestoreSweepBatchSize))
			var refs []*gcf.DocumentRef
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
			n = len(refs)
			for _, ref := range refs {
				if err := tx.Delete(ref); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if n < firestoreSweepBatchSize {
			return nil
		}
	}
}
