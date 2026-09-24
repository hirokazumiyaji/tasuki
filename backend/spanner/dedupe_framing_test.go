package spanner

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Over-budget fallback guards must stay within the STRING(255) budget
// (Codex round-16 on #296): the old fallback wrote the raw long ID, so the
// commit failed with a column violation and the event was never delivered.
// On the old rawFallbackDedupeKey (= identity) the long cases below exceed
// the budget and this fails.
func TestRawFallbackDedupeKeyBounded(t *testing.T) {
	for _, raw := range []string{"x", "__x", "a:b", strings.Repeat("k", 255)} {
		if got := rawFallbackDedupeKey(raw); got != raw {
			t.Fatalf("short ID %q remapped to %q, want identity", raw, got)
		}
	}
	longs := []string{strings.Repeat("k", 256), strings.Repeat("k", 300), strings.Repeat("k", 1000), "__" + strings.Repeat("u", 500)}
	seen := map[string]string{}
	for _, raw := range longs {
		got := rawFallbackDedupeKey(raw)
		if len(got) > dedupeKeyLimit {
			t.Fatalf("fallback for %d-char ID is %d bytes, want <= %d", len(raw), len(got), dedupeKeyLimit)
		}
		if !strings.HasPrefix(got, dedupeHashedUserPrefix) {
			t.Fatalf("fallback %q leaves the %q namespace", got, dedupeHashedUserPrefix)
		}
		if got == escapeDedupeID(raw) {
			t.Fatal("fallback for long ID equals its occupied canonical hash (would collide again)")
		}
		if again := rawFallbackDedupeKey(raw); again != got {
			t.Fatal("fallback derivation is not deterministic")
		}
		if prev, dup := seen[got]; dup {
			t.Fatalf("fallbacks collide for %q and %q", prev, raw)
		}
		seen[got] = raw
		if isPostTerminalMarkerKey(got) {
			t.Fatalf("fallback %q misdetected as marker", got)
		}
	}
}

// The fallback match must key on the bounded fallback form: a long ID's own
// hashed fallback guard matches, while a foreign ID never claims it.
func TestMatchDedupeRowFallbackBounded(t *testing.T) {
	long := strings.Repeat("k", 300)
	fb := rawFallbackDedupeKey(long)
	v2 := spanner.NullInt64{Int64: 2, Valid: true}
	if !matchDedupeRow(long, fb, fb, v2) {
		t.Fatal("bounded fallback guard does not match its owner")
	}
	if matchDedupeRow("x", fb, fb, v2) {
		t.Fatal("bounded fallback guard matched a foreign ID")
	}
	if matchDedupeRow(long, escapeDedupeID(long), fb, v2) {
		t.Fatal("canonical candidate matched a fallback row")
	}
}

// The fallback guard key comes last in the probe order: it is only
// consulted when the canonical key is occupied. Short IDs fall back to the
// raw key itself, so short candidate lists are unchanged.
func TestDedupeKeyCandidatesFallbackLast(t *testing.T) {
	if got := dedupeKeyCandidates("x"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("candidates(x) = %q, want [x]", got)
	}
	got := dedupeKeyCandidates("__x")
	if len(got) != 2 || got[0] != "__x" || got[1] != "____x" {
		t.Fatalf("candidates(__x) = %q, want [__x ____x] (raw first)", got)
	}
	long := strings.Repeat("k", 300)
	lg := dedupeKeyCandidates(long)
	want := []string{long, escapeDedupeID(long), rawFallbackDedupeKey(long)}
	if len(lg) != len(want) {
		t.Fatalf("long candidates = %q, want %q", lg, want)
	}
	for i := range want {
		if lg[i] != want[i] {
			t.Fatalf("long candidates = %q, want %q", lg, want)
		}
	}
}

// A >255-char DedupeID whose hashed canonical key is foreign-occupied must
// still deliver via the bounded fallback guard (Codex round-16 on #296): the
// old fallback wrote the raw long ID into the STRING(255) column, so the
// commit failed and the event was never delivered. On the old code the first
// send below errors and this fails.
func TestDedupeLongFallbackDelivers(t *testing.T) {
	dsn := guardTestDSN(t)
	ctx := context.Background()
	b, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	const id = "spn-long"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("k", 300)
	canonical := escapeDedupeID(long)
	// Foreign-occupy the hashed canonical key with a legacy verbatim row
	// (a pre-escape user ID that literally equals the hash): no
	// format_version, so the round-13 ownership rule treats it as another
	// ID's row and the send must fall back instead of colliding.
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertMap("wf_signal_dedupe", map[string]any{
				"instance_id": id, "dedupe_id": canonical, "created_at": nowUTC(),
			}),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, id, ev, long); err != nil {
		t.Fatalf("send with foreign-occupied hashed key: %v", err)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d, want 1 (event must deliver despite occupied canonical)", len(st.Inbox))
	}
	// The guard lives at the bounded fallback key with v2, and the retry
	// dedupes against it.
	fbRow, err := b.client.Single().ReadRow(ctx, "wf_signal_dedupe",
		spanner.Key{id, rawFallbackDedupeKey(long)}, []string{"dedupe_id", "format_version"})
	if err != nil {
		t.Fatalf("bounded fallback guard missing: %v", err)
	}
	var fbKey string
	var fbVer spanner.NullInt64
	if err := fbRow.Columns(&fbKey, &fbVer); err != nil {
		t.Fatal(err)
	}
	if !fbVer.Valid || fbVer.Int64 != int64(dedupeFormatRawKeyVersion) {
		t.Fatalf("fallback guard version = %v, want %d", fbVer, dedupeFormatRawKeyVersion)
	}
	if err := b.SendToInbox(ctx, id, ev, long); err != nil {
		t.Fatal(err)
	}
	st, err = b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d after retry, want 1 (fallback guard must dedupe)", len(st.Inbox))
	}
}
