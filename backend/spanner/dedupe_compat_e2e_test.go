package spanner_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	backendspanner "github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestCompatRawLegEndToEnd is the round-28 P1 emulator regression test on
// #296 (successor to the round-26 raw-leg test): a new-node running send of
// "__x" must guard SOLELY with the raw legacy "__x" row and leave NO escaped
// "____x" row behind. A pre-upgrade node handling the distinct first send
// DedupeID="____x" probes that exact verbatim key with an existence-only
// read: any canonical row there is mistaken for its own guard and the event
// is silently dropped. Fail-without-fix: the pre-fix write emits the escaped
// primary, so the "____x must be absent" ReadRow below finds it.
func TestCompatRawLegEndToEnd(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := backendspanner.New(ctx, dsn)
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
	raw, err := spanner.NewClient(ctx, os.Getenv("TASUKI_SPANNER_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	// No escaped row: an old node probing "____x" verbatim must miss.
	if _, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "____x"}, []string{"dedupe_id"}); err == nil {
		t.Fatal("escaped row (____x) present: an old-node first send of ____x would mistake it for its own guard and drop the event")
	}
	// Sole raw legacy guard exists, verbatim and version-free.
	row, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "__x"}, []string{"dedupe_id"})
	if err != nil {
		t.Fatalf("sole raw guard (__x) missing: %v (old-node retry would duplicate)", err)
	}
	var stored string
	if err := row.Columns(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "__x" {
		t.Fatalf("sole raw guard stores %q, want verbatim __x", stored)
	}
	// Retry of "__x" still dedupes, and the distinct ID "____x" delivers
	// alongside (both directions coexist without claiming each other).
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
	if _, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "____x"}, []string{"dedupe_id"}); err != nil {
		t.Fatalf("distinct ____x guard missing after delivery: %v", err)
	}
	// A further retry of "__x" still dedupes against the sole guard.
	if err := b.SendToInbox(ctx, inst, ev, "__x"); err != nil {
		t.Fatal(err)
	}
	st, err = b.LoadWorkflow(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d after retry, want 2", len(st.Inbox))
	}
}
