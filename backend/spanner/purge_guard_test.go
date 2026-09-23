package spanner

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func guardTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_SPANNER_DSN")
	if dsn == "" || os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("TASUKI_SPANNER_DSN/SPANNER_EMULATOR_HOST not set")
	}
	return dsn
}

// TestPurgeVictimGuards exercises the purge ID-reuse fence predicates
// directly: the first-sweep guard and the conditional delete must accept only
// the listed incarnation, and the second-sweep guard must stop the moment the
// instance row reappears.
func TestPurgeVictimGuards(t *testing.T) {
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

	const id = "purge-guard-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	row, err := b.client.Single().ReadRow(ctx, "wf_instances", spanner.Key{id}, []string{"created_at"})
	if err != nil {
		t.Fatal(err)
	}
	var createdAt time.Time
	if err := row.Columns(&createdAt); err != nil {
		t.Fatal(err)
	}
	victim := purgeVictim{id: id, createdAt: createdAt}
	stale := purgeVictim{id: id, createdAt: createdAt.Add(-time.Hour)}

	if err := b.checkPurgeVictim(ctx, victim); err != nil {
		t.Fatalf("own incarnation must pass the first-sweep guard: %v", err)
	}
	if err := b.checkPurgeVictim(ctx, stale); !errors.Is(err, errPurgeSuperseded) {
		t.Fatalf("replacement incarnation must trip the first-sweep guard, got %v", err)
	}
	if err := b.checkPurgeAbsent(ctx, id); !errors.Is(err, errPurgeSuperseded) {
		t.Fatalf("present row must trip the second-sweep guard, got %v", err)
	}

	// The conditional delete must stand down for a stale incarnation,
	// leaving the row untouched.
	if done, _, err := b.deletePurgedInstanceRow(ctx, stale); err != nil || done {
		t.Fatalf("stale delete: done=%v err=%v", done, err)
	}
	if _, err := b.GetInstance(ctx, id); err != nil {
		t.Fatalf("stale delete must not remove the row: %v", err)
	}
	// Seed a straggler pair (dedupe key + inbox row) so the atomic residual
	// snapshot taken inside the victim-delete transaction has content to
	// carry: the delete must return exactly the rows observed in its own
	// transaction.
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "sig"}, "guard-key"); err != nil {
		t.Fatal(err)
	}
	if done, residual, err := b.deletePurgedInstanceRow(ctx, victim); err != nil || !done {
		t.Fatalf("own delete: done=%v err=%v", done, err)
	} else {
		if residual == nil || len(residual.dedupe) != 1 || residual.dedupe[0].id != dedupeKey("guard-key") {
			t.Fatalf("own delete must snapshot the straggler dedupe key in-txn, got %+v", residual)
		}
		if len(residual.inboxIDs) != 1 {
			t.Fatalf("own delete must snapshot the straggler inbox row in-txn, got %+v", residual)
		}
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("own delete must remove the row, got %v", err)
	}
	if err := b.checkPurgeAbsent(ctx, id); err != nil {
		t.Fatalf("absent row must pass the second-sweep guard: %v", err)
	}
	// A concurrent purge that arrives after the delete owns nothing.
	if done, _, err := b.deletePurgedInstanceRow(ctx, victim); err != nil || done {
		t.Fatalf("loser delete: done=%v err=%v", done, err)
	}
}
