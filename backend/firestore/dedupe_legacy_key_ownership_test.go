package firestore

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestFramedLegacyStoredKeyCollision pins the aliasing behind issue #296
// the framed doc ID of ("3:3", "x") equals the legacy doc ID of
// ("3", "3:3:x"), so instance "3:3"'s probe for "x" lands exactly on
// instance "3"'s old guard. Ownership validation succeeds for pre-field rows
// (no instance_id to check), so only the stored key tells them apart.
func TestFramedLegacyStoredKeyCollision(t *testing.T) {
	framed := frameDedupeDocID("3:3", "x")
	legacy := legacyDedupeDocID("3", "3:3:x")
	if framed != legacy {
		t.Fatalf("expected the colliding framings to alias, got %q vs %q", framed, legacy)
	}
}

// TestMatchDedupeRowVerifiesLegacyStoredKey checks the legacy-key fix: the
// legacy branch must compare the row's STORED key with the requested raw ID,
// not the probe candidate (which equals the requested ID by construction and
// therefore cannot tell a collision-landed foreign row apart).
func TestMatchDedupeRowVerifiesLegacyStoredKey(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		candidate string
		doc       map[string]any
		want      bool
	}{
		// The finding's pair: instance "3:3" sends "x", the probe lands on
		// instance "3"'s verbatim guard for "3:3:x". The first send must
		// insert, not drop (old code returned true here: candidate "x" ==
		// requested "x").
		{"collision foreign legacy row is not a match", "x", "x",
			map[string]any{"dedupe_id": "3:3:x"}, false},
		// Same collision with an explicit foreign owner: still no match
		// (ownership rejects first, but the key check must agree).
		{"collision foreign legacy row with owner is not a match", "x", "x",
			map[string]any{"instance_id": "3", "dedupe_id": "3:3:x"}, false},
		// Own verbatim guard still matches exactly.
		{"own legacy guard still matches", "x", "x",
			map[string]any{"dedupe_id": "x"}, true},
		// A row missing its stored key never matches (safe direction:
		// duplicates, never drops).
		{"legacy row without stored key is not a match", "x", "x",
			map[string]any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchDedupeRow(tc.raw, tc.candidate, tc.doc); got != tc.want {
				t.Fatalf("matchDedupeRow(%q, %q, %v) = %v, want %v", tc.raw, tc.candidate, tc.doc, got, tc.want)
			}
		})
	}
}

// TestMatchOwnedDedupeRowCollisionPair replays the finding's collision in
// both directions through the ownership-aware matcher: instance "3:3"'s
// first send for "x" must not drop on instance "3"'s old guard, while
// instance "3"'s resend for "3:3:x" must still dedupe on its own guard and
// must not be suppressed by instance "3:3"'s versioned guard for "x".
func TestMatchOwnedDedupeRowCollisionPair(t *testing.T) {
	// Direction 1: ("3:3", "x") probes key "x". The framed doc "3:3:3:x"
	// holds the foreign legacy row; the legacy doc "3:3:x" is absent. The
	// pre-field row (no instance_id) passes ownership, so the stored-key
	// check decides: "3:3:x" != "x" → no match (old code: match → drop).
	foreignLegacy := map[string]any{"dedupe_id": "3:3:x"}
	pr := dedupeKeyProbe{framedDoc: foreignLegacy, legacyDoc: nil}
	if matchOwnedDedupeRow("x", "x", pr, "3:3") {
		t.Fatal(`("3:3", "x") matched the foreign legacy guard: first send would drop`)
	}
	// Direction 2a: ("3", "3:3:x") probes key "3:3:x". The legacy doc
	// "3:3:3:x" holds its OWN verbatim guard → resend dedupes.
	ownLegacy := map[string]any{"instance_id": "3", "dedupe_id": "3:3:x"}
	pr = dedupeKeyProbe{framedDoc: nil, legacyDoc: ownLegacy}
	if !matchOwnedDedupeRow("3:3:x", "3:3:x", pr, "3") {
		t.Fatal(`("3", "3:3:x") missed its own legacy guard: resend would duplicate`)
	}
	// Direction 2b: the same probe landing on ("3:3", "x")'s versioned guard
	// (stored canonical "x", owned by "3:3") must not suppress ("3", "3:3:x"):
	// ownership rejects, and the versioned key check agrees.
	foreignVersioned := map[string]any{"instance_id": "3:3", "dedupe_id": "x", dedupeFormatVersionField: int64(1)}
	pr = dedupeKeyProbe{framedDoc: nil, legacyDoc: foreignVersioned}
	if matchOwnedDedupeRow("3:3:x", "3:3:x", pr, "3") {
		t.Fatal(`("3", "3:3:x") matched ("3:3", "x")'s guard: send would drop`)
	}
}

// TestDedupeFramingCollisionPairEndToEnd replays the finding's collision
// through SendToInboxBatch (framing + ownership + stored-key check together).
// Instance "3"'s verbatim guard for "3:3:x" lives at the legacy doc "3:3:3:x"
// — exactly instance "3:3"'s framed guard doc for "x" — as a pre-field row
// (no instance_id), so ownership validation succeeds and only the stored key
// tells them apart. On the old candidate comparison the first send below is
// skipped and this fails.
func TestDedupeFramingCollisionPairEndToEnd(t *testing.T) {
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
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"3", "3:3"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	// Pre-field verbatim guard for ("3", "3:3:x") at the colliding doc.
	if frameDedupeDocID("3:3", "x") != legacyDedupeDocID("3", "3:3:x") {
		t.Fatal("test setup: collision docs do not alias")
	}
	if _, err := b.ref("wf_signal_dedupe", legacyDedupeDocID("3", "3:3:x")).Create(ctx, map[string]any{
		"dedupe_id":  "3:3:x",
		"created_at": nowUTC(),
	}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	inboxLen := func(id string) int {
		t.Helper()
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return len(st.Inbox)
	}
	// Direction 1: the first send of "x" to "3:3" must deliver despite the
	// aliasing foreign guard.
	if err := b.SendToInbox(ctx, "3:3", ev, "x"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen("3:3"); got != 1 {
		t.Fatalf("collision guard suppressed the first send: inbox=%d, want 1 (signal lost)", got)
	}
	// Direction 2: the resend of "3:3:x" to "3" still dedupes on its own
	// guard (no duplicate from the fix).
	if err := b.SendToInbox(ctx, "3", ev, "3:3:x"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen("3"); got != 0 {
		t.Fatalf("own collision guard stopped matching: inbox=%d, want 0 (duplicate)", got)
	}
}
