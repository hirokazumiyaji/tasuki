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

// TestCompatRawLegTerminalEndToEnd is the round-28 P1 emulator regression
// test on #296 (successor to the round-27 terminal dual-write test): a
// new-node TERMINAL send of "__x" must guard SOLELY with the raw legacy
// "__x" base row and leave NO escaped "____x" row behind — the same
// old-reader isolation as the running path. The round-27 fix dual-wrote the
// raw leg on the terminal path but kept the escaped primary, which a
// pre-upgrade node handling the distinct first send DedupeID="____x" still
// mistakes for its own guard, silently dropping the event.
// Fail-without-fix: the pre-fix terminal write emits the escaped base
// guard, so the "____x must be absent" ReadRow below finds it.
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
	// No escaped base guard: an old node probing "____x" verbatim must miss.
	if _, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "____x"}, []string{"dedupe_id"}); err == nil {
		t.Fatal("terminal escaped base guard (____x) present: an old-node first send of ____x would mistake it for its own guard and drop the event")
	}
	// Sole raw legacy base guard exists.
	row, err := raw.Single().ReadRow(ctx, "wf_signal_dedupe", spanner.Key{inst, "__x"}, []string{"dedupe_id"})
	if err != nil {
		t.Fatalf("terminal sole raw guard (__x) missing: %v (old-node retry would duplicate)", err)
	}
	var stored string
	if err := row.Columns(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "__x" {
		t.Fatalf("terminal sole raw guard stores %q, want verbatim __x", stored)
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
	// The distinct ID "____x" still delivers its own post-terminal event.
	if err := b.SendToInbox(ctx, inst, ev, "____x"); err != nil {
		t.Fatal(err)
	}
	st, err = b.LoadWorkflow(ctx, inst)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Inbox) != 2 {
		t.Fatalf("inbox=%d after distinct ____x terminal send, want 2", len(st.Inbox))
	}
}
