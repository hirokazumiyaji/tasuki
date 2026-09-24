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

// TestCompatRawLegTerminalEndToEnd is the round-27 P1 emulator regression
// test on #296: a new-node TERMINAL send of "__x" must leave, besides the
// escaped "____x" base guard, a raw legacy "__x" row so an old-node retry
// probing only the verbatim key hits. The round-26 fix dual-wrote the raw
// leg on the running path only; the terminal path wrote the escaped primary
// alone (miss, then duplicate on old-node retry).
// Fail-without-fix: the pre-fix terminal write is escaped-only, so the raw
// ReadRow below is NotFound.
func TestCompatRawLegTerminalEndToEnd(t *testing.T) {
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
	inst := fmt.Sprintf("rawleg-term-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: inst, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, inst); err != nil {
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
		t.Fatalf("terminal escaped primary (____x) missing: %v", err)
	}
	// Raw legacy compat leg exists.
	row, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "__x"}, []string{"dedupe_id"})
	if err != nil {
		t.Fatalf("terminal raw compat leg (__x) missing: %v (old-node retry would duplicate)", err)
	}
	var stored string
	if err := row.Columns(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "__x" {
		t.Fatalf("terminal raw compat leg stores %q, want verbatim __x", stored)
	}
	// Retry dedupes via the post-terminal marker (still one inbox event).
	if err := b.SendToInbox(ctx, inst, ev, "__x"); err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 1 {
		t.Fatalf("inbox=%d after terminal retry, want 1", len(st.Inbox))
	}
}
