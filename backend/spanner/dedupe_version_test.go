package spanner

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// matchDedupeRow must identify a row's owner positively (Codex round 13 on
// #296): versioned rows match only their canonical owner, legacy (NULL
// version) rows only their exact raw ID — never another ID's escaped form.
func TestMatchDedupeRow(t *testing.T) {
	longUnder := "__" + strings.Repeat("u", 300)
	longHashed := escapeDedupeID(longUnder)
	v1 := spanner.NullInt64{Int64: 1, Valid: true}
	v2 := spanner.NullInt64{Int64: 2, Valid: true}
	legacy := spanner.NullInt64{}
	cases := []struct {
		name      string
		raw       string
		candidate string
		stored    string
		version   spanner.NullInt64
		want      bool
	}{
		{"versioned own via escaped candidate", "__x", "____x", "____x", v1, true},
		{"versioned foreign via raw candidate", "____x", "____x", "____x", v1, false},
		{"versioned own plain ID", "x", "x", "x", v1, true},
		{"versioned hashed own", longUnder, longHashed, longHashed, v1, true},
		{"versioned hash-like raw is not the hashed row", "__hash__:abc", "__hash__:abc", "__hash__:abc", v1, false},
		{"legacy exact raw", "__x", "__x", "__x", legacy, true},
		{"legacy plain exact raw", "x", "x", "x", legacy, true},
		{"legacy row is not another ID's escaped form", "__x", "____x", "____x", legacy, false},
		{"legacy row is not another ID's raw form", "____x", "__x", "__x", legacy, false},
		{"raw-key fallback own", "__fresh", "__fresh", "__fresh", v2, true},
		{"raw-key fallback foreign via escaped candidate", "__x", "____x", "____x", v2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchDedupeRow(tc.raw, tc.candidate, tc.stored, tc.version); got != tc.want {
				t.Fatalf("matchDedupeRow(%q, %q, %q, %v) = %v, want %v",
					tc.raw, tc.candidate, tc.stored, tc.version, got, tc.want)
			}
		})
	}
}

// Distinct user IDs whose stored forms collide textually ("__x" stored as
// "____x", which is also the verbatim form of "____x") must dedupe
// independently in both send orders (Codex round 13 on #296). On the old
// dual-read probe the second ID's first send hit the first ID's row and was
// skipped (wrong owner), so the inbox stays at 1 and this fails.
func TestDedupeEscapedPairBothDirections(t *testing.T) {
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
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	send := func(id, dedupe string) {
		t.Helper()
		if err := b.SendToInbox(ctx, id, ev, dedupe); err != nil {
			t.Fatalf("send %q to %s: %v", dedupe, id, err)
		}
	}

	const fwd = "dedupe-pair-fwd"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: fwd, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	send(fwd, "__x")
	if n := terminalTestInboxLen(t, b, ctx, fwd); n != 1 {
		t.Fatalf("forward setup inbox=%d want 1", n)
	}
	send(fwd, "__x")
	if n := terminalTestInboxLen(t, b, ctx, fwd); n != 1 {
		t.Fatalf("own-ID resend duplicated: inbox=%d want 1", n)
	}
	send(fwd, "____x")
	if n := terminalTestInboxLen(t, b, ctx, fwd); n != 2 {
		t.Fatalf("distinct ID swallowed by foreign row: inbox=%d want 2 (signal lost)", n)
	}
	send(fwd, "____x")
	send(fwd, "__x")
	if n := terminalTestInboxLen(t, b, ctx, fwd); n != 2 {
		t.Fatalf("pair idempotency lost: inbox=%d want 2", n)
	}

	const rev = "dedupe-pair-rev"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: rev, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	send(rev, "____x")
	if n := terminalTestInboxLen(t, b, ctx, rev); n != 1 {
		t.Fatalf("reverse setup inbox=%d want 1", n)
	}
	send(rev, "__x")
	if n := terminalTestInboxLen(t, b, ctx, rev); n != 2 {
		t.Fatalf("reverse distinct ID swallowed: inbox=%d want 2 (signal lost)", n)
	}
	send(rev, "__x")
	send(rev, "____x")
	if n := terminalTestInboxLen(t, b, ctx, rev); n != 2 {
		t.Fatalf("reverse pair idempotency lost: inbox=%d want 2", n)
	}
}

// Legacy (pre-versioning, verbatim, NULL format_version) rows guard only
// their exact raw ID (Codex round 13 on #296): own-ID resends still dedupe,
// but a legacy row never suppresses another ID's escaped-form send. The rows
// are inserted directly without format_version to simulate pre-upgrade data.
// On the old dual-read probe the "__fresh" send below hits the "____fresh"
// legacy row and is skipped, so the inbox stays at 2 and this fails.
func TestDedupeLegacyRowExactRawMatch(t *testing.T) {
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

	const id = "dedupe-legacy-exact"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	seedLegacy := func(key string) {
		t.Helper()
		if _, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertMap("wf_signal_dedupe", map[string]any{
					"instance_id": id, "dedupe_id": key, "created_at": nowUTC(),
				}),
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
	seedLegacy("__leg")
	seedLegacy("____leg")
	// A legacy row at the requested ID's escaped form, owned by another ID.
	seedLegacy("____fresh")
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	send := func(dedupe string) {
		t.Helper()
		if err := b.SendToInbox(ctx, id, ev, dedupe); err != nil {
			t.Fatalf("send %q: %v", dedupe, err)
		}
	}

	// Own-ID sends dedupe against their legacy guards (compat: a legacy row
	// is a prior send's guard).
	send("__leg")
	if n := terminalTestInboxLen(t, b, ctx, id); n != 0 {
		t.Fatalf("legacy own-ID guard missed: inbox=%d want 0 (duplicate)", n)
	}
	send("____leg")
	if n := terminalTestInboxLen(t, b, ctx, id); n != 0 {
		t.Fatalf("legacy own-ID guard missed: inbox=%d want 0 (duplicate)", n)
	}
	// A legacy row is never another ID's escaped form: "__fresh" escapes to
	// "____fresh", whose legacy row belongs to "____fresh" — the first send
	// must insert (via the raw-key fallback guard), not drop, and the retry
	// must dedupe.
	send("__fresh")
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("send swallowed by foreign legacy row: inbox=%d want 1 (signal lost)", n)
	}
	send("__fresh")
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("fallback guard missed the retry: inbox=%d want 1 (duplicate)", n)
	}
	// The legacy row's owner still dedupes against it.
	send("____fresh")
	if n := terminalTestInboxLen(t, b, ctx, id); n != 1 {
		t.Fatalf("legacy owner guard missed: inbox=%d want 1 (duplicate)", n)
	}
}
