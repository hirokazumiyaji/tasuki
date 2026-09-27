package firestore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func guardTestEmulator(t *testing.T) {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set")
	}
}

// TestPurgeVictimGuards exercises the purge ID-reuse fence predicates
// directly: the first-sweep guard and the conditional delete must accept only
// the listed incarnation, and the second-sweep guard must stop the moment the
// instance doc reappears.
func TestPurgeVictimGuards(t *testing.T) {
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
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const id = "purge-guard-1"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	snap, err := b.ref("wf_instances", id).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	victim := purgeVictim{id: id, createdAt: timestamp(snap.Data(), "created_at")}
	stale := purgeVictim{id: id, createdAt: victim.createdAt.Add(-time.Hour)}

	if err := b.checkPurgeVictim(ctx, victim); err != nil {
		t.Fatalf("own incarnation must pass the first-sweep guard: %v", err)
	}
	if err := b.checkPurgeVictim(ctx, stale); !errors.Is(err, errPurgeSuperseded) {
		t.Fatalf("replacement incarnation must trip the first-sweep guard, got %v", err)
	}
	if err := b.checkPurgeAbsent(ctx, id); !errors.Is(err, errPurgeSuperseded) {
		t.Fatalf("present doc must trip the second-sweep guard, got %v", err)
	}

	// The conditional delete must stand down for a stale incarnation,
	// leaving the doc (and the seq counter) untouched.
	if done, _, _, err := b.deletePurgedInstanceDoc(ctx, stale); err != nil || done {
		t.Fatalf("stale delete: done=%v err=%v", done, err)
	}
	if _, err := b.GetInstance(ctx, id); err != nil {
		t.Fatalf("stale delete must not remove the doc: %v", err)
	}
	// Seed a straggler pair (dedupe key + inbox row) so the atomic residual
	// snapshot taken inside the victim-delete transaction has content to
	// carry: the delete must return exactly the rows observed in its own
	// transaction.
	if err := b.SendToInbox(ctx, id, journal.Event{Type: journal.TypeSignalReceived, Name: "sig"}, "guard-key"); err != nil {
		t.Fatal(err)
	}
	if done, residual, _, err := b.deletePurgedInstanceDoc(ctx, victim); err != nil || !done {
		t.Fatalf("own delete: done=%v err=%v", done, err)
	} else {
		if residual == nil || len(residual.dedupe) != 1 || residual.dedupe[0].ref.ID != signalDedupeID(id, "guard-key") {
			t.Fatalf("own delete must snapshot the straggler dedupe row in-txn, got %+v", residual)
		}
		if len(residual.inbox) != 1 {
			t.Fatalf("own delete must snapshot the straggler inbox row in-txn, got %+v", residual)
		}
	}
	if _, err := b.GetInstance(ctx, id); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("own delete must remove the doc, got %v", err)
	}
	if err := b.checkPurgeAbsent(ctx, id); err != nil {
		t.Fatalf("absent doc must pass the second-sweep guard: %v", err)
	}
	// A concurrent purge that arrives after the delete owns nothing.
	if done, _, _, err := b.deletePurgedInstanceDoc(ctx, victim); err != nil || done {
		t.Fatalf("loser delete: done=%v err=%v", done, err)
	}
}
