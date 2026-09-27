package spanner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

// commitTimestampOptionSet reports whether allow_commit_timestamp is set on
// a column (test helper for the round-22 P2 migration assertion).
func commitTimestampOptionSet(t *testing.T, b *Backend, ctx context.Context, table, column string) bool {
	t.Helper()
	iter := b.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM INFORMATION_SCHEMA.COLUMN_OPTIONS
			WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table AND COLUMN_NAME = @column AND OPTION_NAME = 'allow_commit_timestamp' LIMIT 1`,
		Params: map[string]any{"table": table, "column": column},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// TestCommitTimestampOptionsCoverSweepColumns covers the Codex round-22 P2
// (e) schema story on #291: the terminal-sweep ordering columns must accept
// the commit-timestamp placeholder, so cutoffs compare server commit order
// instead of writer wall clocks. Fresh databases get the option from
// schema.sql; pre-existing ones gain it from Migrate
// (ensureCommitTimestampOptions). Without the option, every commitTimestamp
// write fails and no cutoff is commit-ordered.
func TestCommitTimestampOptionsCoverSweepColumns(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	for _, c := range []struct{ table, column string }{
		{"wf_instances", "sweep_commit_ts"},
		{"wf_signal_dedupe", "created_at"},
		{"wf_tasks", "created_at"},
		{"wf_inbox", "created_at"},
	} {
		if !commitTimestampOptionSet(t, b, ctx, c.table, c.column) {
			t.Errorf("allow_commit_timestamp missing on %s.%s (sweep cutoff falls back to skewed client clocks)", c.table, c.column)
		}
	}
}

// readInstanceTimes returns an instance's sweep_commit_ts/updated_at.
func readInstanceTimes(t *testing.T, b *Backend, ctx context.Context, id string) (sweepTs spanner.NullTime, updatedAt time.Time) {
	t.Helper()
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"sweep_commit_ts", "updated_at"})
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Columns(&sweepTs, &updatedAt); err != nil {
		t.Fatal(err)
	}
	return sweepTs, updatedAt
}

// TestTerminalCutoffUsesCommitOrder covers the Codex round-22 P2 (e)
// behavior on #291 end to end through the public API: the terminal flip
// records the status commit's own commit timestamp in sweep_commit_ts (not
// a writer wall clock), and a send accepted after the flip carries a later
// commit timestamp, so the post-commit sweep preserves it by commit order
// no matter how the two workers' clocks skew. completed_at deliberately
// stays a client timestamp for retention purges (see commitTimestamp);
// only the sweep tick is commit-ordered.
//
// sweep_commit_ts != updated_at pins the mechanism: stamping the tick from
// the client clock would reproduce updated_at, while the placeholder
// resolves to the commit time (never bit-equal to the client stamp at
// nanosecond precision across a commit round trip).
func TestTerminalCutoffUsesCommitOrder(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	id := fmt.Sprintf("r22-sp-committs-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "old"}, "old"); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC()
	if err := b.CommitAdvancement(ctx, claimTerminalAdv(t, b, ctx, id, "w1")); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()

	completedAt, updatedAt := readInstanceTimes(t, b, ctx, id)
	if !completedAt.Valid {
		t.Fatal("terminal commit left no sweep_commit_ts")
	}
	// Sanity window only (generous: TrueTime commit timestamps carry
	// uncertainty against this client's clock — the emulator's runs
	// ~150ms ahead — so exact containment in [before, after] is not
	// guaranteed): the tick must be this commit's timestamp, not zero or
	// a stale value. The sharp pin is the inequality with updated_at
	// below.
	if completedAt.Time.Before(before.Add(-5*time.Minute)) || completedAt.Time.After(after.Add(5*time.Minute)) {
		t.Errorf("sweep_commit_ts = %v, want near the CommitAdvancement window [%v, %v] (not the commit's own timestamp)", completedAt.Time, before, after)
	}
	if completedAt.Time.Equal(updatedAt) {
		t.Error("sweep_commit_ts == updated_at: the flip reused the client entry-time stamp instead of the status commit's commit timestamp")
	}

	// A send accepted after the flip (dedupe marker plus inbox row) must
	// carry a later commit timestamp and survive the post-commit sweep.
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "new"}
	if err := b.SendToInboxBatch(ctx, id, []backend.InboxItem{{Event: ev, DedupeID: "new"}}); err != nil {
		t.Fatal(err)
	}
	var sendCreated time.Time
	func() {
		iter := b.client.Single().Query(ctx, spanner.Statement{
			SQL:    `SELECT created_at FROM wf_inbox WHERE instance_id = @id`,
			Params: map[string]any{"id": id},
		})
		defer iter.Stop()
		row, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&sendCreated); err != nil {
			t.Fatal(err)
		}
	}()
	if sendCreated.Before(completedAt.Time) {
		t.Errorf("post-commit send created_at = %v, want at or after the flip %v (commit order violated: the sweep would delete the accepted send)", sendCreated, completedAt.Time)
	}
	if err := b.cleanupTerminalInstance(ctx, id, true); err != nil {
		t.Fatal(err)
	}
	if n := countChildRows(t, b, ctx, "wf_inbox", id); n != 1 {
		t.Errorf("inbox rows after sweep = %d, want 1 (the commit-ordered post-commit send)", n)
	}
	if n := countChildRows(t, b, ctx, "wf_signal_dedupe", id); n != 1 {
		t.Errorf("dedupe rows after sweep = %d, want 1 (marker preserved with its inbox row)", n)
	}
}

// TestMigrateAddsSweepCommitTsColumn covers the ADD COLUMN retrofit path:
// a database created before sweep_commit_ts existed gains it from Migrate.
// Skips when the emulator cannot DROP COLUMN to simulate the pre-fix
// shape (the option-backfill test above still covers the ensure path).
func TestMigrateAddsSweepCommitTsColumn(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	if err := b.applyDDL(ctx, []string{`ALTER TABLE wf_instances DROP COLUMN sweep_commit_ts`}); err != nil {
		t.Skipf("emulator cannot drop the column, cannot simulate a pre-fix database: %v", err)
	}
	exists, err := b.columnExists(ctx, "wf_instances", "sweep_commit_ts")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("setup: column still present after dropping it")
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if exists, err := b.columnExists(ctx, "wf_instances", "sweep_commit_ts"); err != nil || !exists {
		t.Fatalf("Migrate did not re-add sweep_commit_ts (exists=%v, err=%v)", exists, err)
	}
	if !commitTimestampOptionSet(t, b, ctx, "wf_instances", "sweep_commit_ts") {
		t.Error("re-added sweep_commit_ts lacks allow_commit_timestamp")
	}
}

// TestMigrateBackfillsCommitTimestampOptions covers the retrofit path: a
// database created before the option existed (option explicitly cleared)
// gains it from Migrate, mirroring the round-19/20 P2 index backfills.
func TestMigrateBackfillsCommitTimestampOptions(t *testing.T) {
	b, ctx := fenceTestBackend(t)
	clearStmt := `ALTER TABLE wf_inbox ALTER COLUMN created_at SET OPTIONS (allow_commit_timestamp=null)`
	if err := b.applyDDL(ctx, []string{clearStmt}); err != nil {
		t.Skipf("emulator cannot clear the option, cannot simulate a pre-fix database: %v", err)
	}
	if commitTimestampOptionSet(t, b, ctx, "wf_inbox", "created_at") {
		t.Fatal("setup: option still set after clearing it")
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if !commitTimestampOptionSet(t, b, ctx, "wf_inbox", "created_at") {
		t.Error("Migrate did not restore allow_commit_timestamp on wf_inbox.created_at")
	}
}
