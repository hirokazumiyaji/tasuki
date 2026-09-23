package sqlite

import (
	"context"
	"testing"
	"time"
)

// TestMigrationProcessLockPerDatabase pins the per-database in-process
// guard: unrelated database files migrate concurrently, the same file
// serializes, and a canceled context returns instead of blocking. SQLite
// has no advisory lock, so this guard (plus BEGIN IMMEDIATE) is the
// exclusion for same-file migrations.
func TestMigrationProcessLockPerDatabase(t *testing.T) {
	ctx := context.Background()
	relA, err := acquireMigrationProcessLock(ctx, "test-migrate-file-a")
	if err != nil {
		t.Fatal(err)
	}
	// Unrelated database proceeds concurrently.
	relB, err := acquireMigrationProcessLock(ctx, "test-migrate-file-b")
	if err != nil {
		t.Fatal(err)
	}
	relB()
	// Same database blocks until released.
	acquired := make(chan func(), 1)
	go func() {
		rel, err := acquireMigrationProcessLock(context.Background(), "test-migrate-file-a")
		if err != nil {
			return
		}
		acquired <- rel
	}()
	select {
	case <-acquired:
		t.Fatal("same-database lock acquired while held")
	case <-time.After(50 * time.Millisecond):
	}
	relA()
	select {
	case rel := <-acquired:
		rel()
	case <-time.After(2 * time.Second):
		t.Fatal("same-database lock not acquired after release")
	}
	// Canceled context does not block behind a held lock.
	relC, err := acquireMigrationProcessLock(ctx, "test-migrate-file-c")
	if err != nil {
		t.Fatal(err)
	}
	defer relC()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireMigrationProcessLock(canceled, "test-migrate-file-c"); err == nil {
		t.Fatal("canceled context must not acquire the lock")
	}
}
