package firestore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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
// round-21 P1 on #296: created_at equality as incarnation identity breaks
// on clock rollback, VM restore, or precision truncation — a recreated ID
// can carry the SAME created_at as the purged victim, and a stale sweep
// comparing only created_at then deletes the replacement's documents.
// The test recreates the ID with the victim's exact created_at but a fresh
// incarnation token (as any CreateInstance does) and resumes the paused
// sweep with the stale fence: it must abort with nil leaving every
// replacement row intact. Old code compared created_at only, saw equality,
// and deleted the replacement's task, timer, and dedupe guard.
func TestTerminalSweepSameCreatedAtReplacementAborts(t *testing.T) {
	guardTestEmulator(t)
	ctx := context.Background()
	b, err := New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Unique per run (no Reset: the shared emulator may host concurrent
	// suites); scoped cleanup at the end.
	id := fmt.Sprintf("incarnation-fence-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = b.ref("wf_instances", id).Delete(ctx)
		_, _ = b.ref("wf_tasks", wfTaskID(id)).Delete(ctx)
	})
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
	stale := instanceVictim(t, b, ctx, id)
	if stale.incarnation == "" {
		t.Fatal("setup: instance doc carries no incarnation token")
	}
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("dedupe snapshot holds %d keys, want the single guard K", len(snapshot))
	}

	// Simulate purge-delete plus clock-rollback recreate: the replacement
	// carries the victim's EXACT created_at (rollback/VM restore/truncation
	// reproduced it) but a fresh token, as every CreateInstance mints.
	orig, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ref("wf_instances", id).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	replacement := orig.Data()
	replacement[incarnationField] = newIncarnation()
	replacement["status"] = "running"
	if _, err := b.ref("wf_instances", id).Set(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	cur := instanceVictim(t, b, ctx, id)
	if !cur.createdAt.Equal(stale.createdAt) {
		t.Fatal("setup: replacement must reproduce the victim's created_at exactly")
	}
	if cur.incarnation == stale.incarnation {
		t.Fatal("setup: replacement must carry a fresh incarnation token")
	}
	// The purge reaped the old incarnation's child rows with the victim;
	// drop them so only the replacement's own rows remain (otherwise the
	// resent guard below would dedupe-hit the old row instead of recreating
	// the replacement's).
	_, _ = b.ref("wf_tasks", wfTaskID(id)).Delete(ctx)
	_, _ = b.ref("wf_timers", journalID(id, 7)).Delete(ctx)
	_, _ = b.ref("wf_signal_dedupe", signalDedupeID(id, "K")).Delete(ctx)
	it := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
	for {
		d, err := it.Next()
		if err != nil {
			break
		}
		_, _ = d.Ref.Delete(ctx)
	}
	it.Stop()
	// The replacement's rows (dedupe doc IDs are deterministic, so the
	// resent guard recreates the very same document the snapshot names).
	if err := b.SendToInbox(ctx, id, ev, "K"); err != nil {
		t.Fatal(err)
	}
	seedFenceTimer(t, b, ctx, id, 8)
	replacementDocs := map[string]string{
		"task":   wfTaskID(id),
		"timer":  journalID(id, 8),
		"dedupe": signalDedupeID(id, "K"),
	}
	cols := map[string]string{"task": "wf_tasks", "timer": "wf_timers", "dedupe": "wf_signal_dedupe"}
	for kind, docID := range replacementDocs {
		if !fenceDocExists(t, b, ctx, cols[kind], docID) {
			t.Fatalf("setup: replacement %s doc %s/%s missing", kind, cols[kind], docID)
		}
	}

	// The paused sweep resumes with its stale pre-commit fence: same
	// created_at, different token. It must abort (nil) instead of deleting
	// the replacement's rows.
	if err := b.sweepTerminateDocs(ctx, stale, snapshot); err != nil {
		t.Fatalf("stale terminate sweep: %v (want fenced abort to nil)", err)
	}
	if err := b.sweepSignalDedupeIDs(ctx, purgeFence{victim: stale}, snapshot); err != nil {
		t.Fatalf("stale dedupe sweep: %v (want fenced abort to nil)", err)
	}
	for kind, docID := range replacementDocs {
		if !fenceDocExists(t, b, ctx, cols[kind], docID) {
			t.Fatalf("stale sweep deleted the replacement's %s doc %s/%s despite the token mismatch", kind, cols[kind], docID)
		}
	}

	// Positive control: with the CURRENT incarnation the same sweep still
	// cleans up.
	flipStatusWithoutSweep(t, b, ctx, id)
	cur = instanceVictim(t, b, ctx, id)
	curSnapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.sweepTerminateDocs(ctx, cur, curSnapshot); err != nil {
		t.Fatal(err)
	}
	for kind, docID := range replacementDocs {
		if fenceDocExists(t, b, ctx, cols[kind], docID) {
			t.Fatalf("current-incarnation sweep left %s doc %s/%s behind", kind, cols[kind], docID)
		}
	}
}
