package spanner_test

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	backendspanner "github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestRound18BatchFallbackKeysDistinct is the end-to-end round-18 P2 case:
// batch IDs "____x" + "__x" with foreign-occupied "______x" (a legacy
// verbatim guard of user ID "______x" at the first item's canonical key —
// owned by the same instance but never this ID's guard). The first item
// falls back to "____x"; the second item's canonical probe cannot see the
// buffered mutation, so without the batch reservation both emit the same
// insert. The batch must commit with distinct guards and the retry must
// dedupe via them.
func TestRound18BatchFallbackKeysDistinct(t *testing.T) {
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
	const inst = "r18b-spanner"
	// Best-effort cleanup for re-runs against a non-wiped emulator.
	_ = b.TerminateInstance(ctx, inst)
	_, _ = b.PurgeInstances(ctx, 0, nil, 100)
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: inst, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	// Seed the foreign legacy row directly: key "______x" is the verbatim
	// guard of user ID "______x" (no version/owner columns), so it occupies
	// "____x"'s canonical slot without matching it.
	raw, err := spanner.NewClient(ctx, os.Getenv("TASUKI_SPANNER_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("wf_signal_dedupe", []string{"instance_id", "dedupe_id", "created_at"},
			[]any{inst, "______x", time.Now().UTC()}),
	}); err != nil {
		t.Fatal(err)
	}
	sig := func(id string) backend.InboxItem {
		return backend.InboxItem{
			Event:    journal.Event{Type: journal.TypeSignalReceived, Name: "sig"},
			DedupeID: id,
		}
	}
	inboxLen := func() int {
		t.Helper()
		st, err := b.LoadWorkflow(ctx, inst)
		if err != nil {
			t.Fatal(err)
		}
		return len(st.Inbox)
	}
	batch := []backend.InboxItem{sig("____x"), sig("__x")}
	if err := b.SendToInboxBatch(ctx, inst, batch); err != nil {
		t.Fatalf("batch with colliding fallback keys must commit: %v", err)
	}
	if n := inboxLen(); n != 2 {
		t.Fatalf("inbox after batch = %d, want 2", n)
	}
	// Retrying the batch must dedupe via the two distinct guards.
	if err := b.SendToInboxBatch(ctx, inst, batch); err != nil {
		t.Fatal(err)
	}
	if n := inboxLen(); n != 2 {
		t.Fatalf("inbox after batch retry = %d, want 2 (must dedupe)", n)
	}
}
