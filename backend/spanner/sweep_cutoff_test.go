package spanner

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The post-commit dedupe sweep must preserve keys created after the terminal
// transition (Codex round 2 on #327). notifyTerminal fires before the sweep,
// so a concurrent SendToInbox with a new DedupeID can land inside the sweep
// window; deleting its key while the inbox event remains would duplicate a
// later retry of the same DedupeID.
func TestSweepSignalDedupePreservesPostCommitKeys(t *testing.T) {
	dsn := guardTestDSN(t)
	ctx := context.Background()
	b, err := New(ctx, dsn)
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

	const id = "dedupe-cutoff"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	commit := time.Now().UTC()
	put := func(dedupeID string, createdAt time.Time) {
		t.Helper()
		_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertMap("wf_signal_dedupe", map[string]any{
					"instance_id": id, "dedupe_id": dedupeID, "created_at": createdAt,
				}),
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	put("pre-commit", commit.Add(-time.Second))
	put("post-commit", commit.Add(time.Second))

	if err := b.sweepSignalDedupe(ctx, id, nil, commit); err != nil {
		t.Fatal(err)
	}
	if b.dedupeKeyExists(ctx, id, "pre-commit") {
		t.Fatal("pre-commit dedupe key survived the sweep")
	}
	if !b.dedupeKeyExists(ctx, id, "post-commit") {
		t.Fatal("post-commit dedupe key was swept; a later retry would duplicate the signal")
	}
}

func (b *Backend) dedupeKeyExists(ctx context.Context, instanceID, dedupeID string) bool {
	_, err := b.client.Single().ReadRow(ctx, "wf_signal_dedupe",
		spanner.Key{instanceID, dedupeID}, []string{"dedupe_id"})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false
		}
		return false
	}
	return true
}
