package firestore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// TestTerminalInboxBatchLimit caps 100-item terminal signal batches
// (round-25 P2): worst-case 5 writes per item plus the seq flush exceeds
// Firestore's 500-write limit, so the batch must fail fast with
// ErrBatchTooLarge instead of a deterministic transaction-limit error.
func TestTerminalInboxBatchLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := newBackend(t)
	if err := b.Reset(ctx); err != nil {
		t.Skip(err.Error())
	}
	const id = "terminal-batch-limit"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "wf", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	if err := b.TerminateInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	mkBatch := func(n int) []backend.InboxItem {
		items := make([]backend.InboxItem, 0, n)
		for i := 0; i < n; i++ {
			items = append(items, backend.InboxItem{
				Event:    journal.Event{Type: journal.TypeSignalReceived, Name: fmt.Sprintf("sig-%d", i)},
				DedupeID: fmt.Sprintf("terminal-batch-%d", i),
			})
		}
		return items
	}
	if err := b.SendToInboxBatch(ctx, id, mkBatch(100)); !errors.Is(err, backend.ErrBatchTooLarge) {
		t.Fatalf("100-item terminal batch: err=%v, want ErrBatchTooLarge", err)
	}
	// The cap itself must still deliver.
	if err := b.SendToInboxBatch(ctx, id, mkBatch(99)); err != nil {
		t.Fatalf("99-item terminal batch: err=%v, want nil", err)
	}
}
