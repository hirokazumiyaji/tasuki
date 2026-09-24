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

// TestDualWriteRawLegacyEndToEnd is the round-28 P1 emulator regression test
// on #296 (successor to the round-26 dual-write test): a new-node send of
// "__x" must guard at the framed fallback doc plus the RAW legacy doc
// "<instance>:__x", and must leave NO canonical-shaped doc behind — neither
// the framed "N:<instance>:____x" primary nor, crucially, the legacy
// "<instance>:____x" doc. A pre-upgrade node handling the distinct first
// send DedupeID="____x" probes that exact verbatim doc ID with an
// existence-only read: any canonical-shaped row there is mistaken for its
// own guard and the event is silently dropped. Fail-without-fix: the pre-fix
// pick prefers the framed canonical doc, so the framed-canonical Get below
// finds it. Uses a unique instance ID and scoped cleanup (no Reset) so it is
// safe against the shared emulator.
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
	// No canonical-shaped docs: an old node probing "____x" verbatim under
	// either framing must miss.
	if snap, err := b.ref("wf_signal_dedupe", signalDedupeID(inst, "__x")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("framed canonical guard present: ambiguous IDs must not materialize canonical docs")
	}
	if snap, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(inst, "____x")).Get(ctx); err == nil && snap.Exists() {
		t.Fatal("legacy canonical guard present: an old-node first send of ____x would mistake it for its own guard and drop the event")
	}
	// Framed fallback guard exists with v2 and the stored canonical form.
	fbSnap, err := b.ref("wf_signal_dedupe", frameDedupeDocID(inst, "__x")).Get(ctx)
	if err != nil || !fbSnap.Exists() {
		t.Fatalf("framed fallback guard %q missing (err=%v)", frameDedupeDocID(inst, "__x"), err)
	}
	fbDoc := fbSnap.Data()
	if !docInstanceMatches(fbDoc, inst) || !matchDedupeRow("__x", "__x", fbDoc) {
		t.Fatalf("framed fallback guard does not resolve for __x: %+v", fbDoc)
	}
	// Raw legacy counterpart exists — the old-reader leg: a pre-framing node
	// probing "<instance>:__x" hits by document existence.
	rawSnap, err := b.ref("wf_signal_dedupe", legacyDedupeDocID(inst, "__x")).Get(ctx)
	if err != nil || !rawSnap.Exists() {
		t.Fatalf("raw legacy guard %q missing (err=%v): old-node retry would duplicate", legacyDedupeDocID(inst, "__x"), err)
	}
	// Retry dedupes, and the distinct ID "____x" delivers alongside (both
	// directions coexist without claiming each other's guards).
	if err := b.SendToInbox(ctx, inst, ev, "__x"); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToInbox(ctx, inst, ev, "____x"); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d, want 2 (retry deduped, distinct ____x delivered)", len(st.Inbox))
	}
}
