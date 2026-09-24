package spanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

// purgeChildlessVerifyRounds bounds the sweep→verify-delete rounds of one
// purge victim (Codex round-19 P2 on #291). Each round drains every child
// table and then deletes the parent only if no child row raced in; a round
// that observes raced rows re-sweeps. Sustained concurrent writes to a
// terminal instance could spin forever, so an exhausted bound fails with the
// parent intact — the next purge reselects it — instead of blocking the
// batch. Terminal purge victims are old (retention cutoff) and quiescent in
// practice; a victim under live writes is retried, never orphaned.
const purgeChildlessVerifyRounds = 5

// errPurgeWriteRace reports a victim whose verify-delete kept observing
// raced child rows through every round. The parent row is intact, so the
// failure is recoverable by retry; it must never be mistaken for a purged
// instance (the count is not incremented — see PurgeInstances).
var errPurgeWriteRace = errors.New("spanner: purge raced concurrent writers; parent left intact for retry")

// purgeChildBatchBudget caps the child-row deletions committed in one purge
// transaction, and purgeChildBatchPage bounds the rows taken from a single
// table within it (Codex round-16 on #291). Cloud Spanner allows 20,000
// mutations per commit, but PurgeInstances used to delete ALL children of ALL
// selected instances with DML statements in ONE read-write transaction: with
// the default selection limit (1000 instances) the commit exceeds the
// mutation limit and the purge fails on every retry. Purge now mirrors the
// terminal bounded-cleanup pattern on this branch
// (terminalCleanupMutationBudget / terminalCleanupSweepBatch): per-instance
// bounded deletes with commits between pages, so every commit stays far
// below the limit no matter how many rows an instance holds.
const (
	purgeChildBatchBudget = 1000
	purgeChildBatchPage   = 500
)

// PurgeInstances deletes up to lim terminal instances and their dependent
// rows in per-instance bounded transactions: each instance's children are
// swept in budgeted pages (deletePurgeBatch) with commits between pages, so
// a large journal or inbox never exceeds the per-commit mutation limit.
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := nowUTC().Add(-olderThan)
	// Selection is a read-only snapshot: per-instance purges below commit
	// independently, so holding the candidate set in one read-write
	// transaction would only inflate contention and abort rates.
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT id FROM wf_instances
		      WHERE status IN UNNEST(@sts)
		        AND completed_at IS NOT NULL AND completed_at <= @cutoff
		      ORDER BY completed_at, id LIMIT @limit`,
		Params: map[string]any{"sts": sts, "cutoff": cutoff, "limit": int64(lim)},
	})
	var ids []string
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

// purgeOneInstance removes one instance and all its child rows. Children go
// first so a crash mid-purge leaves a parent whose leftovers a later purge
// still selects; the parent delete then commits atomically with a childless
// verification, and no second sweep follows (Codex round-19 P2 on #291).
// The old second sweep ran AFTER the parent delete: a failure or exit there
// left parent-less child rows no later purge could reselect (the victim
// query keys on wf_instances), orphaning them permanently while reporting
// success for the children cleaned so far. The verify-delete below closes
// that window structurally instead of with a marker: the emptiness check
// and the parent delete commit in ONE transaction, so a writer committing
// between the sweep drain and the delete either aborts this transaction
// (its rows are re-swept next round) or aborts itself — SendToInboxBatch
// reads wf_instances inside its transaction, so once the parent delete
// commits no new child rows can appear (in-flight writers lose the race
// and retry into ErrNotFound). A failure at any point before the delete
// leaves the parent intact for retry; after the delete there is nothing
// left to recover.
func (b *Backend) purgeOneInstance(ctx context.Context, id string) error {
	if err := purgeVictimRounds(
		func() error { return b.purgeInstanceChildren(ctx, id) },
		func() (bool, error) { return b.deletePurgeVictimIfChildless(ctx, id) },
		purgeChildlessVerifyRounds,
	); err != nil {
		return fmt.Errorf("spanner: purge of %s: %w", id, err)
	}
	return nil
}

// purgeVictimRounds drives one victim's sweep→verify-delete cycle: drain
// every child table, then atomically verify emptiness and delete the
// parent. A round that observes raced-in rows re-sweeps; an exhausted round
// bound fails with errPurgeWriteRace (parent intact). Sweep or verification
// errors propagate immediately. Pure seam for unit tests (see
// purge_rounds_test.go); the backend method above supplies the Spanner
// closures.
func purgeVictimRounds(sweep func() error, verifyDelete func() (bool, error), maxRounds int) error {
	for round := 0; round < maxRounds; round++ {
		if err := sweep(); err != nil {
			return err
		}
		gone, err := verifyDelete()
		if err != nil {
			return err
		}
		if gone {
			return nil
		}
	}
	return errPurgeWriteRace
}

// deletePurgeVictimIfChildless deletes the instance and its inbox-seq row
// iff no child row remains, atomically: the LIMIT 1 existence probes and
// the deletes commit in one read-write transaction (see purgeOneInstance
// for why no post-delete sweep can leave orphans). It reports gone=false
// when raced-in rows were observed (caller re-sweeps); a commit conflict
// from a racing writer surfaces as an error and the client library retries
// the closure before it ever reaches the caller.
func (b *Backend) deletePurgeVictimIfChildless(ctx context.Context, id string) (gone bool, err error) {
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		gone = false
		empty, err := purgeChildrenEmpty(ctx, txn, id)
		if err != nil {
			return err
		}
		if !empty {
			return nil
		}
		if err := txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{id}),
			spanner.Delete("wf_instances", spanner.Key{id}),
		}); err != nil {
			return err
		}
		gone = true
		return nil
	})
	return gone, err
}

// purgeChildrenEmpty reports whether every purge-owned child table holds no
// row for the instance. One LIMIT 1 probe per table keeps the verification
// transaction tiny; the probes' ranges also invalidate this transaction
// against a writer committing into them first (commit conflict → retry →
// re-sweep), which is what makes the verify-delete atomic.
func purgeChildrenEmpty(ctx context.Context, txn *spanner.ReadWriteTransaction, id string) (bool, error) {
	probes := []spanner.Statement{
		{SQL: `SELECT dedupe_id FROM wf_signal_dedupe WHERE instance_id = @id LIMIT 1`, Params: map[string]any{"id": id}},
		{SQL: `SELECT id FROM wf_tasks WHERE instance_id = @id LIMIT 1`, Params: map[string]any{"id": id}},
		{SQL: `SELECT seq FROM wf_timers WHERE instance_id = @id LIMIT 1`, Params: map[string]any{"id": id}},
		{SQL: `SELECT id FROM wf_inbox WHERE instance_id = @id LIMIT 1`, Params: map[string]any{"id": id}},
		{SQL: `SELECT seq FROM wf_journal WHERE instance_id = @id LIMIT 1`, Params: map[string]any{"id": id}},
	}
	for _, st := range probes {
		iter := txn.Query(ctx, st)
		_, err := iter.Next()
		iter.Stop()
		if err == iterator.Done {
			continue
		}
		if err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// purgeInstanceChildren deletes every child row of one instance in budgeted
// transactions until none remain.
func (b *Backend) purgeInstanceChildren(ctx context.Context, id string) error {
	for {
		n, err := b.deletePurgeBatch(ctx, id)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// purgePageLimit bounds the rows taken from one table of a purge batch:
// at most purgeChildBatchPage, and never more than the transaction's
// remaining shared budget. Pure for unit tests.
func purgePageLimit(remaining int) int { return min(remaining, purgeChildBatchPage) }

// deletePurgeBatch deletes up to purgeChildBatchBudget child rows of one
// instance in one transaction and reports how many rows were removed. The
// per-table page keeps any single table from consuming the whole budget, and
// the shared budget caps the transaction total across tables — mirroring the
// terminal-cleanup accounting (deleteTerminalBatch), plus the journal table
// purge owns that terminal cleanup leaves behind.
func (b *Backend) deletePurgeBatch(ctx context.Context, id string) (int, error) {
	var n int
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n = 0
		var muts []*spanner.Mutation
		remaining := purgeChildBatchBudget
		take := func(page []*spanner.Mutation) {
			muts = append(muts, page...)
			remaining -= len(page)
		}
		dMuts, err := deleteSignalDedupe(ctx, txn, id, purgePageLimit(remaining), time.Time{}, false) // purge owns the instance: unbounded
		if err != nil {
			return err
		}
		take(dMuts)
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, purgePageLimit(remaining), time.Time{}, false)
		if err != nil {
			return err
		}
		take(tMuts)
		tmMuts, err := deleteTimersForInstance(ctx, txn, id, purgePageLimit(remaining))
		if err != nil {
			return err
		}
		take(tmMuts)
		inMuts, err := deleteInboxForInstance(ctx, txn, id, purgePageLimit(remaining), time.Time{}, false)
		if err != nil {
			return err
		}
		take(inMuts)
		jMuts, err := deleteJournalForInstance(ctx, txn, id, purgePageLimit(remaining))
		if err != nil {
			return err
		}
		take(jMuts)
		n = len(muts)
		return txn.BufferWrite(muts)
	})
	return n, err
}

// deleteJournalForInstance returns deletions for up to limit journal rows of
// the instance. Purge must page the journal like every other child table:
// instances accumulate journal rows over many turns, so an unbounded journal
// delete would blow the per-commit mutation budget on its own.
func deleteJournalForInstance(ctx context.Context, txn *spanner.ReadWriteTransaction, instanceID string, limit int) ([]*spanner.Mutation, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL:    `SELECT seq FROM wf_journal WHERE instance_id = @id LIMIT @limit`,
		Params: map[string]any{"id": instanceID, "limit": int64(limit)},
	})
	defer iter.Stop()
	var muts []*spanner.Mutation
	for {
		r, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var seq int64
		if err := r.Columns(&seq); err != nil {
			return nil, err
		}
		muts = append(muts, spanner.Delete("wf_journal", spanner.Key{instanceID, seq}))
	}
	return muts, nil
}
