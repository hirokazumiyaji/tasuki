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

// The post-commit dedupe sweep must preserve keys first appearing after the
// pre-commit snapshot (Codex round 3 on #327). Classification follows
// transaction serialization order, not SendToInbox client timestamps: a send
// that captures created_at before the terminal commit, loses the race, and
// retry-commits after it must keep its key — deleting it while the inbox
// event remains would duplicate a later retry of the same DedupeID.
func TestSweepSignalDedupeSnapshotPreservesPostCommitKeys(t *testing.T) {
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

	const id = "dedupe-snapshot"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
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
	put("pre-commit", time.Now().UTC().Add(-time.Second))

	// Snapshot the pre-commit set, exactly as CommitAdvancements does before
	// its terminal commit.
	snapshot, err := b.listSignalDedupeIDs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot[0] != "pre-commit" {
		t.Fatalf("snapshot = %v, want the single pre-commit key", snapshot)
	}

	// A racing send landing after the snapshot — even one stamping a stale
	// pre-commit client timestamp after losing a transaction race — must be
	// preserved: its key was never in the committed set.
	put("post-commit", time.Now().UTC().Add(-time.Second))
	put("post-commit-fresh", time.Now().UTC().Add(time.Second))

	// The fenced sweep (Codex round 20 on #296) still removes exactly the
	// snapshot while the instance carries its pre-commit incarnation.
	var createdAt time.Time
	func() {
		row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at"})
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&createdAt); err != nil {
			t.Fatal(err)
		}
	}()
	victim := purgeVictim{id: id, createdAt: createdAt}
	guard := func(ctx context.Context) error { return b.checkPurgeVictim(ctx, victim) }
	guardTx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return b.checkPurgeVictimTx(ctx, txn, victim)
	}
	if err := b.sweepSignalDedupeIDs(ctx, id, guard, guardTx, snapshot); err != nil {
		t.Fatal(err)
	}
	if b.dedupeKeyExists(ctx, id, "pre-commit") {
		t.Fatal("pre-commit dedupe key survived the sweep")
	}
	for _, key := range []string{"post-commit", "post-commit-fresh"} {
		if !b.dedupeKeyExists(ctx, id, key) {
			t.Fatalf("post-snapshot dedupe key %q was swept; a later retry would duplicate the signal", key)
		}
	}
}

func (b *Backend) dedupeKeyExists(ctx context.Context, instanceID, dedupeID string) bool {
	_, err := b.client.Single().ReadRow(ctx, "wf_signal_dedupe",
		spanner.Key{instanceID, dedupeKey(dedupeID)}, []string{"dedupe_id"})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false
		}
		return false
	}
	return true
}
