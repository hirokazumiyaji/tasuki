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

// TestDualWriteRawLegacyEndToEnd is the round-26 P1 emulator regression test
// on #296: a new-node send of "__x" must leave a guard at the RAW legacy doc
// "<instance>:__x" (not just the framed primary and the encoded legacy
// "<instance>:____x"), so an old-node retry probing only the raw form hits.
// Fail-without-fix: the pre-fix dual wrote legacy(canonical), so the raw-doc
// Get below is NotFound. Uses a unique instance ID and scoped cleanup (no
// Reset) so it is safe against the shared emulator.
func TestDualWriteRawLegacyEndToEnd(t *testing.T) {
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
	inst := fmt.Sprintf("rawleg-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: inst, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	ev := journal.Event{Type: journal.TypeSignalReceived, Name: "sig", Payload: []byte(`{}`)}
	if err := b.SendToInbox(ctx, inst, ev, "__x"); err != nil {
		t.Fatal(err)
	}
	// Framed primary exists.
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(inst, "__x")).Get(ctx); err != nil || !snap.Exists() {
		t.Fatalf("framed primary missing (err=%v)", err)
	}
	// Raw legacy counterpart exists — the old-reader leg.
	rawSnap, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(inst, "__x")).Get(ctx)
	if err != nil || !rawSnap.Exists() {
		t.Fatalf("raw legacy guard %q missing (err=%v): old-node retry would duplicate", legacyDedupeDocID(inst, "__x"), err)
	}
	// Old-reader simulation against the raw doc exactly as a pre-framing
	// (version-aware) node probes it: raw doc ID, raw candidate.
	rawDoc := rawSnap.Data()
	if !docInstanceMatches(rawDoc, inst) || !matchDedupeRow("__x", "__x", rawDoc) {
		t.Fatalf("old reader probing %q misses the guard: %+v", legacyDedupeDocID(inst, "__x"), rawDoc)
	}
	// Retry dedupes (still one inbox event).
	if err := b.SendToInbox(ctx, inst, ev, "__x"); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d after retry, want 1", len(st.Inbox))
	}
}
