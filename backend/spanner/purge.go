package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/api/iterator"
)

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
// still selects; the parent delete then commits, and a second sweep reaps
// writers that committed between the first sweep and the parent delete
// (SendToInboxBatch reads wf_instances inside its transaction, so once the
// parent delete commits no new child rows can appear — in-flight writers
// lose the race and retry into ErrNotFound).
func (b *Backend) purgeOneInstance(ctx context.Context, id string) error {
	if err := b.purgeInstanceChildren(ctx, id); err != nil {
		return err
	}
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Delete("wf_inbox_seq", spanner.Key{id}),
			spanner.Delete("wf_instances", spanner.Key{id}),
		})
	})
	if err != nil {
		return err
	}
	return b.purgeInstanceChildren(ctx, id)
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
		dMuts, err := deleteSignalDedupe(ctx, txn, id, purgePageLimit(remaining))
		if err != nil {
			return err
		}
		take(dMuts)
		tMuts, err := deleteTasksForInstance(ctx, txn, id, 0, purgePageLimit(remaining))
		if err != nil {
			return err
		}
		take(tMuts)
		tmMuts, err := deleteTimersForInstance(ctx, txn, id, purgePageLimit(remaining))
		if err != nil {
			return err
		}
		take(tmMuts)
		inMuts, err := deleteInboxForInstance(ctx, txn, id, purgePageLimit(remaining))
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
