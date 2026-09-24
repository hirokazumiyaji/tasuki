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

// TestCompatRawLegEndToEnd is the round-26 P1 emulator regression test on
// #296: a new-node running send of "__x" must leave, besides the escaped
// "____x" primary, a raw legacy "__x" row so an old-node retry probing only
// the verbatim key hits. Fail-without-fix: the pre-fix write is
// escaped-only, so the raw ReadRow below is NotFound.
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
	// Escaped primary exists.
	if _, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "____x"}, []string{"dedupe_id"}); err != nil {
		t.Fatalf("escaped primary (____x) missing: %v", err)
	}
	// Raw legacy compat leg exists.
	row, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "__x"}, []string{"dedupe_id"})
	if err != nil {
		t.Fatalf("raw compat leg (__x) missing: %v (old-node retry would duplicate)", err)
	}
	var stored string
	if err := row.Columns(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "__x" {
		t.Fatalf("raw compat leg stores %q, want verbatim __x", stored)
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
