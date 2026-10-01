package spanner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestVictimMatches pins the incarnation identity predicate (Codex
// round-21 P1 on #296): tokened incarnations compare tokens exactly, and
// legacy (tokenless) sides fall back to created_at equality.
func TestVictimMatches(t *testing.T) {
	created := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		v    purgeVictim
		cur  time.Time
		tok  string
		want bool
	}{
		{"same token matches despite clock skew", purgeVictim{id: "a", createdAt: created, incarnation: "tok-1"}, created.Add(time.Hour), "tok-1", true},
		{"same created_at with different token mismatches", purgeVictim{id: "a", createdAt: created, incarnation: "tok-1"}, created, "tok-2", false},
		{"legacy victim falls back to created_at", purgeVictim{id: "a", createdAt: created}, created, "tok-9", true},
		{"legacy victim rejects different created_at", purgeVictim{id: "a", createdAt: created}, created.Add(time.Hour), "tok-9", false},
		{"legacy current falls back to created_at", purgeVictim{id: "a", createdAt: created, incarnation: "tok-1"}, created, "", true},
		{"legacy current rejects different created_at", purgeVictim{id: "a", createdAt: created, incarnation: "tok-1"}, created.Add(time.Hour), "", false},
		{"both legacy compare created_at", purgeVictim{id: "a", createdAt: created}, created, "", true},
	}
	for _, c := range cases {
		if got := victimMatches(c.v, c.cur, c.tok); got != c.want {
			t.Errorf("%s: victimMatches = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestNewIncarnationUnique guards the token entropy: consecutive tokens
// must differ (a reused token would alias incarnations exactly like a
// reused created_at).
func TestNewIncarnationUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		tok := newIncarnation()
		if tok == "" {
			t.Fatal("newIncarnation returned empty token")
		}
		if len(tok) < 16 {
			t.Fatalf("newIncarnation token %q too short to be unique", tok)
		}
		if seen[tok] {
			t.Fatalf("newIncarnation reused token %q", tok)
		}
		seen[tok] = true
	}
}

// TestTerminalSweepSameCreatedAtReplacementAborts covers the Codex
// round-21 P1 on #296, mirrored from Firestore: created_at equality as
// incarnation identity breaks on clock rollback, VM restore, or precision
// truncation — a recreated ID can carry the SAME created_at as the purged
// victim, and a stale sweep comparing only created_at then deletes the
// replacement's rows. The test recreates the row with the victim's exact
// created_at but a fresh incarnation token (as any CreateInstance does) and
// resumes the paused sweep with the stale fence: it must abort with nil
// leaving every replacement row intact. Old code compared created_at only,
// saw equality, and deleted the replacement's task, timer, and guard.
func TestTerminalSweepSameCreatedAtReplacementAborts(t *testing.T) {
	b, ctx, _ := commitTerminateTestBackend(t)

	const id = "incarnation-fence"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	seedFenceTimer(t, b, ctx, id, 7)
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	// What the terminal commit observes: the pre-commit incarnation (token
	// plus created_at) and the exact dedupe key set.
	stale := instanceVictimTx(t, b, ctx, id)
	if stale.incarnation == "" {
		t.Fatal("setup: instance row carries no incarnation token")
	}
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0] != dedupeKey("K") {
		t.Fatalf("dedupe snapshot = %v, want the single guard K", snapshot)
	}

	// Simulate purge-delete plus clock-rollback recreate: the replacement
	// row carries the victim's EXACT created_at (rollback/VM
	// restore/truncation reproduced it) but a fresh token, as every
	// CreateInstance mints.
	if _, err := b.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("wf_instances", spanner.Key{id}),
	}); err != nil {
		t.Fatal(err)
	}
	replacementToken := newIncarnation()
	if _, err := b.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap("wf_instances", map[string]any{
			"id": id, "name": "WF", "queue": "default", "status": "running",
			"next_seq": int64(2), "created_at": stale.createdAt,
			"updated_at": nowUTC(), "incarnation": replacementToken,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	cur := instanceVictimTx(t, b, ctx, id)
	if !cur.createdAt.Equal(stale.createdAt) {
		t.Fatal("setup: replacement must reproduce the victim's created_at exactly")
	}
	if cur.incarnation == stale.incarnation {
		t.Fatal("setup: replacement must carry a fresh incarnation token")
	}
	// The purge reaped the old incarnation's child rows with the victim;
	// drop them so only the replacement's own rows remain.
	if err := b.withRW(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		for _, table := range []string{"wf_tasks", "wf_timers", "wf_signal_dedupe", "wf_inbox"} {
			if _, err := txn.Update(ctx, spanner.Statement{
				SQL:    `DELETE FROM ` + table + ` WHERE instance_id = @id`,
				Params: map[string]any{"id": id},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The replacement's rows (dedupe keys are deterministic, so the resent
	// guard recreates the very same row the snapshot names).
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	seedFenceTimer(t, b, ctx, id, 8)
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 1 {
		t.Fatalf("setup: replacement tasks = %d, want 1", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 1 {
		t.Fatalf("setup: replacement timers = %d, want 1", n)
	}
	if !b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("setup: replacement guard K missing")
	}

	// The paused sweep resumes with its stale pre-commit fence: same
	// created_at, different token. It must abort (nil) instead of deleting
	// the replacement's rows.
	if err := b.sweepTerminateDocs(ctx, stale, snapshot); err != nil {
		t.Fatalf("stale terminate sweep: %v (want fenced abort to nil)", err)
	}
	guard := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, stale) }
	guardTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, stale)
	}
	if err := b.sweepSignalDedupeIDs(ctx, id, guard, guardTx, snapshot); err != nil {
		t.Fatalf("stale dedupe sweep: %v (want fenced abort to nil)", err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 1 {
		t.Fatalf("stale sweep deleted the replacement's workflow task despite the token mismatch (tasks=%d, want 1)", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 1 {
		t.Fatalf("stale sweep deleted the replacement's timer despite the token mismatch (timers=%d, want 1)", n)
	}
	if !b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("stale sweep deleted the replacement's guard K despite the token mismatch")
	}

	// Positive control: with the CURRENT incarnation the same sweep still
	// cleans up.
	flipStatusWithoutSweep(t, b, ctx, id)
	cur = instanceVictimTx(t, b, ctx, id)
	curSnapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.sweepTerminateDocs(ctx, cur, curSnapshot); err != nil {
		t.Fatal(err)
	}
	if n := fenceChildCount(t, b, ctx, "wf_tasks", "id", id); n != 0 {
		t.Fatalf("current-incarnation sweep left %d task rows behind", n)
	}
	if n := fenceChildCount(t, b, ctx, "wf_timers", "seq", id); n != 0 {
		t.Fatalf("current-incarnation sweep left %d timer rows behind", n)
	}
	if b.dedupeKeyExists(ctx, id, "K") {
		t.Fatal("current-incarnation sweep left guard K behind")
	}
}

// TestMigrateBackfillsIncarnationColumn covers the legacy path of the
// round-21 P1 incarnation fix: databases created before the column existed
// gain it via Migrate (nullable; existing rows read NULL), and fences fall
// back to created_at comparison for those tokenless rows.
func TestMigrateBackfillsIncarnationColumn(t *testing.T) {
	dsn := guardTestDSN(t)
	ctx := context.Background()
	legacyDSN := dsn + "-legacy-r21"
	if err := RecreateDatabase(ctx, legacyDSN); err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, legacyDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	// Pre-fix schema: wf_instances without the incarnation column (and a
	// wf_inbox without the seq column, as of the same era).
	if err := b.applyDDL(ctx, []string{`
CREATE TABLE wf_instances (
  id STRING(255) NOT NULL,
  name STRING(255) NOT NULL,
  queue STRING(255) NOT NULL DEFAULT ('default'),
  status STRING(64) NOT NULL DEFAULT ('running'),
  input JSON,
  next_seq INT64 NOT NULL DEFAULT (1),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP
) PRIMARY KEY (id)`, `
CREATE TABLE wf_inbox (
  id INT64 NOT NULL,
  instance_id STRING(255) NOT NULL,
  type STRING(64) NOT NULL,
  ref_seq INT64,
  payload JSON,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id)`, `
CREATE TABLE wf_tasks (
  id INT64 NOT NULL,
  kind STRING(32) NOT NULL,
  queue STRING(255) NOT NULL DEFAULT ('default'),
  instance_id STRING(255) NOT NULL,
  ref_seq INT64,
  payload JSON,
  attempt INT64 NOT NULL DEFAULT (0),
  max_attempts INT64,
  visible_at TIMESTAMP NOT NULL,
  worker_id STRING(255),
  heartbeat BYTES(MAX),
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id)`}); err != nil {
		t.Fatal(err)
	}
	if exists, err := b.columnExists(ctx, "wf_instances", incarnationColumn); err != nil || exists {
		t.Fatalf("setup: incarnation column exists=%v err=%v, want false/nil", exists, err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if exists, err := b.columnExists(ctx, "wf_instances", incarnationColumn); err != nil || !exists {
		t.Fatalf("Migrate must backfill the incarnation column, exists=%v err=%v", exists, err)
	}
	// Migrate stays idempotent with the column present.
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	// A legacy row (no token) still fences by created_at.
	legacyAt := time.Date(2026, 3, 3, 3, 3, 3, 0, time.UTC)
	if _, err := b.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap("wf_instances", map[string]any{
			"id": "legacy-1", "name": "WF", "queue": "default", "status": "completed",
			"next_seq": int64(2), "created_at": legacyAt,
			"updated_at": legacyAt, "completed_at": legacyAt,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.checkPurgeVictim(ctx, purgeVictim{id: "legacy-1", createdAt: legacyAt}); err != nil {
		t.Fatalf("legacy row must match by created_at fallback: %v", err)
	}
	if err := b.checkPurgeVictim(ctx, purgeVictim{id: "legacy-1", createdAt: legacyAt.Add(time.Hour)}); !errors.Is(err, errPurgeSuperseded) {
		t.Fatalf("legacy row with different created_at must trip the fence, got %v", err)
	}
}
