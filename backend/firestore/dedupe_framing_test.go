package firestore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// Doc-ID framing must be injective in (instance, key) (Codex round-16 on
// #296): the old instanceID + ":" + key concatenation aliased pairs like
// ("a", "b:c") and ("a:b", "c") onto "a:b:c", and lookups never validated
// the stored instance_id, so one instance's legacy row could suppress
// another instance's send. On the old framing every colliding pair below
// shares one document and this fails.
func TestSignalDedupeDocIDInjectiveAcrossInstances(t *testing.T) {
	pairs := [][2]string{
		{"a", "b:c"},
		{"a:b", "c"},
		{"a", "x:__post_terminal__v1:y"},
		{"1:a", "c"},
		{"12", "x:k"},
		{"3:3:x", "k"},
		{"", "a:b"},
		{":", ":"},
	}
	seenUser := map[string][2]string{}
	seenMarker := map[string][2]string{}
	for _, p := range pairs {
		inst, dedupe := p[0], p[1]
		if doc := signalDedupeID(inst, dedupe); true {
			if prev, dup := seenUser[doc]; dup && prev != p {
				t.Fatalf("user docs collide: %q and %q share %q", prev, p, doc)
			}
			seenUser[doc] = p
		}
		mdoc := postTerminalMarkerDocID(inst, dedupe)
		if prev, dup := seenMarker[mdoc]; dup && prev != p {
			t.Fatalf("marker docs collide: %q and %q share %q", prev, p, mdoc)
		}
		seenMarker[mdoc] = p
	}
	// The headline collision: distinct pairs, distinct documents.
	if d1, d2 := signalDedupeID("a", "b:c"), signalDedupeID("a:b", "c"); d1 == d2 {
		t.Fatalf("cross-instance user docs collide at %q", d1)
	}
	if d1, d2 := postTerminalMarkerDocID("a", "x:y"), postTerminalMarkerDocID("a:x", "y"); d1 == d2 {
		t.Fatalf("cross-instance marker docs collide at %q", d1)
	}
	// Framed probes for one pair never address the other's documents; the
	// legacy concatenation still aliases (that is the fixed bug), which is
	// why every hit is gated on docInstanceMatches.
	framed := func(inst string, dedupe string) map[string]bool {
		out := map[string]bool{}
		for _, bk := range dedupeKeyCandidates(dedupe) {
			out[frameDedupeDocID(inst, bk)] = true
		}
		return out
	}
	for doc := range framed("a", "b:c") {
		if framed("a:b", "c")[doc] {
			t.Fatalf("framed probe sets overlap at %q", doc)
		}
	}
}

// Framed doc IDs must round-trip to their (instance, key) pair, however
// adversarial the components (colons, leading digits, empty, multibyte),
// and malformed/legacy IDs must not parse as framed.
func TestSplitDedupeDocIDRoundTrip(t *testing.T) {
	instances := []string{"", "a", "a:b", "12", "1:a", "3:3:x", ":", "::", "日本語", strings.Repeat("i", 300)}
	keys := []string{"", "x", "a:b", "__x", "1:k", strings.Repeat("k", 300), escapeDedupeID(strings.Repeat("k", 300)), postTerminalDedupeMarker("x")}
	for _, inst := range instances {
		for _, k := range keys {
			doc := frameDedupeDocID(inst, k)
			ri, rk, ok := splitDedupeDocID(doc)
			if !ok || ri != inst || rk != k {
				t.Fatalf("frame(%q, %q) = %q round-trips to (%q, %q, %v)", inst, k, doc, ri, rk, ok)
			}
			suffix, ok := dedupeDocKeySuffix(doc, inst)
			if !ok || suffix != k {
				t.Fatalf("dedupeDocKeySuffix(%q, %q) = (%q, %v), want (%q, true)", doc, inst, suffix, ok, k)
			}
			// A foreign instance never extracts a suffix from a framed ID.
			if _, ok := dedupeDocKeySuffix(doc, inst+"\x00"); ok {
				t.Fatalf("dedupeDocKeySuffix(%q, foreign) unexpectedly matched", doc)
			}
		}
	}
	for _, bad := range []string{"", "abc", ":", "4:12", "4:12:x", "-1:a:k", "x:a:k", "999:a"} {
		if _, _, ok := splitDedupeDocID(bad); ok {
			t.Fatalf("splitDedupeDocID(%q) unexpectedly parsed as framed", bad)
		}
	}
	// Legacy IDs keep working through the suffix fallback.
	if s, ok := dedupeDocKeySuffix("a:b:c", "a"); !ok || s != "b:c" {
		t.Fatalf("legacy suffix = (%q, %v), want (b:c, true)", s, ok)
	}
}

// Over-budget fallback guards must stay within the shared STRING(255)
// budget (Codex round-16 on #296): the old fallback wrote the raw long ID,
// which Spanner rejects — the commit fails and the event is never
// delivered. On the old rawFallbackDedupeKey (= identity) the long cases
// below exceed the budget and this fails.
func TestRawFallbackDedupeKeyBounded(t *testing.T) {
	for _, raw := range []string{"x", "__x", "a:b", strings.Repeat("k", 255)} {
		if got := rawFallbackDedupeKey(raw); got != raw {
			t.Fatalf("short ID %q remapped to %q, want identity", raw, got)
		}
	}
	longs := []string{strings.Repeat("k", 256), strings.Repeat("k", 1000), "__" + strings.Repeat("u", 500)}
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
			t.Fatalf("fallback for long ID equals its occupied canonical hash (would collide again)")
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

// The v2 fallback match must identify its owner positively: the candidate
// must be this ID's fallback key AND the stored form must name this ID
// (canonical, or the raw key for pre-refinement rows) — so deliberately
// reusing another send's fallback hash as your own DedupeID cannot claim
// its guard.
func TestMatchDedupeRowFallbackBounded(t *testing.T) {
	long := strings.Repeat("k", 300)
	fb := rawFallbackDedupeKey(long)
	own := map[string]any{"dedupe_id": escapeDedupeID(long), dedupeFormatVersionField: int64(2)}
	if !matchDedupeRow(long, fb, own) {
		t.Fatal("bounded fallback guard does not match its owner")
	}
	// Hash-reuse confusion: another ID's fallback key with a foreign stored
	// form never matches.
	confused := map[string]any{"dedupe_id": escapeDedupeID("something-else"), dedupeFormatVersionField: int64(2)}
	if matchDedupeRow("x", fb, confused) {
		t.Fatal("fallback key with foreign stored form matched")
	}
	if matchDedupeRow("x", fb, map[string]any{"dedupe_id": fb, dedupeFormatVersionField: int64(2)}) {
		t.Fatal("fallback key with self-naming stored form matched a foreign ID")
	}
	// Pre-refinement short rows (stored raw) still match their owner.
	legacy := map[string]any{"dedupe_id": "__fresh", dedupeFormatVersionField: int64(2)}
	if !matchDedupeRow("__fresh", "__fresh", legacy) {
		t.Fatal("pre-refinement short fallback row stopped matching its owner")
	}
}

// Ownership validation must accept own and field-less rows but reject rows
// stored for another instance (defense in depth with framing).
func TestDocInstanceMatches(t *testing.T) {
	if !docInstanceMatches(map[string]any{"instance_id": "a"}, "a") {
		t.Fatal("own row rejected")
	}
	if !docInstanceMatches(map[string]any{}, "a") {
		t.Fatal("field-less row rejected (must fall back to key/version match)")
	}
	if docInstanceMatches(map[string]any{"instance_id": "a:b"}, "a") {
		t.Fatal("foreign row accepted")
	}
}

// A legacy (unversioned, old-framed) guard of one instance must never
// suppress another instance's send, even when the legacy doc IDs alias
// (Codex round-16 on #296): ("frm-a", "sub:c") and ("frm-a:sub", "c") share
// "frm-a:sub:c". On the old lookup (no stored-instance validation) the
// first send below is skipped and this fails.
func TestDedupeCrossInstanceLegacyIsolation(t *testing.T) {
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
	for _, id := range []string{"frm-a", "frm-a:sub"} {
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	// Legacy verbatim guard for ("frm-a:sub", "c"): old framing, no version.
	if _, err := b.ref("wf_signal_dedupe", legacyDedupeDocID("frm-a:sub", "c")).Create(ctx, map[string]any{
		"instance_id": "frm-a:sub",
		"dedupe_id":   "c",
		"created_at":  time.Now().UTC(),
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
	// The foreign legacy row must not suppress ("frm-a", "sub:c").
	if err := b.SendToInbox(ctx, "frm-a", ev, "sub:c"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen("frm-a"); got != 1 {
		t.Fatalf("cross-instance legacy row suppressed the send: inbox=%d, want 1", got)
	}
	// ...but it still guards its owner: a resend of "c" to "frm-a:sub" is a
	// duplicate, not a delivery.
	if err := b.SendToInbox(ctx, "frm-a:sub", ev, "c"); err != nil {
		t.Fatal(err)
	}
	if got := inboxLen("frm-a:sub"); got != 0 {
		t.Fatalf("legacy guard stopped matching its owner: inbox=%d, want 0", got)
	}
}

// A >255-char DedupeID whose hashed canonical key is foreign-occupied must
// still deliver via the bounded fallback guard (Codex round-16 on #296): the
// old fallback wrote the raw long ID, which exceeds the shared STRING(255)
// budget (fatal on Spanner; the framed-assertion below also fails on old
// Firestore code, which wrote the legacy-framed raw key).
func TestDedupeLongFallbackDelivers(t *testing.T) {
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
	const id = "frm-long"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("k", 300)
	canonical := escapeDedupeID(long)
	// Foreign-occupy the hashed canonical key with a legacy verbatim row
	// (a pre-escape user ID that literally equals the hash).
	if _, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(id, canonical)).Create(ctx, map[string]any{
		"instance_id": id,
		"dedupe_id":   canonical,
		"created_at":  time.Now().UTC(),
	}); err != nil {
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
	// The guard lives at the bounded framed fallback key with v2, and the
	// retry dedupes against it.
	fbDoc := frameDedupeDocID(id, rawFallbackDedupeKey(long))
	snap, err := b.ref("wf_signal_dedupe", fbDoc).Get(ctx)
	if err != nil || !snap.Exists() {
		t.Fatalf("bounded fallback guard %q missing (err=%v)", fbDoc, err)
	}
	if v, _ := snap.Data()["format_version"].(int64); v != int64(dedupeFormatRawKeyVersion) {
		t.Fatalf("fallback guard version = %v, want %d", snap.Data()["format_version"], dedupeFormatRawKeyVersion)
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
