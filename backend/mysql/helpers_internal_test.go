package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// TestIsDuplicateColumnError pins the strict ALTER handling in Migrate: only
// MySQL error 1060 (duplicate column) may be tolerated for ADD COLUMN
// backfills. Every other failure (missing table 1146, permissions 1142,
// syntax 1064) must abort the migration instead of being silently ignored.
func TestIsDuplicateColumnError(t *testing.T) {
	if isDuplicateColumnError(nil) {
		t.Fatal("nil is not a duplicate-column error")
	}
	if !isDuplicateColumnError(&driver.MySQLError{Number: 1060, Message: "Duplicate column name 'heartbeat'"}) {
		t.Fatal("want error 1060 to be duplicate column")
	}
	for _, number := range []uint16{1146, 1142, 1064} {
		if isDuplicateColumnError(&driver.MySQLError{Number: number, Message: "other failure"}) {
			t.Fatalf("error %d must not be a duplicate-column error", number)
		}
	}
}

// TestScopedMigrationLockName pins the per-database migration lock scope:
// unrelated databases on the same server must map to distinct locks, every
// name must fit MySQL's 64-character GET_LOCK limit, and with no database
// selected the server-wide base name is kept.
func TestScopedMigrationLockName(t *testing.T) {
	if got := scopedMigrationLockName(""); got != migrateLockName {
		t.Fatalf("empty database: got %q, want %q", got, migrateLockName)
	}
	if got, want := scopedMigrationLockName("tasuki"), "tasuki_migrate_lock_tasuki"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	a, b := scopedMigrationLockName("tasuki_a"), scopedMigrationLockName("tasuki_b")
	if a == b {
		t.Fatalf("distinct databases must map to distinct locks, both %q", a)
	}
	if scopedMigrationLockName("tasuki_a") != a {
		t.Fatal("lock name must be deterministic")
	}
	// Database names may contain characters outside the lock-safe alphabet
	// (dashes, dots, slashes); they must be sanitized away.
	sanitized := scopedMigrationLockName("my-db.v2/x")
	for _, r := range sanitized {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '$'
		if !ok {
			t.Fatalf("lock name %q contains unsafe rune %q", sanitized, r)
		}
	}
	// Overlong database names must still fit the 64-character GET_LOCK
	// limit, and distinct long names sharing a prefix must stay distinct
	// via the hash suffix.
	longA := strings.Repeat("a", 100)
	longB := strings.Repeat("a", 99) + "b"
	nameA, nameB := scopedMigrationLockName(longA), scopedMigrationLockName(longB)
	for _, name := range []string{nameA, nameB} {
		if len(name) > maxMigrationLockLen {
			t.Fatalf("lock name %q exceeds %d characters", name, maxMigrationLockLen)
		}
	}
	if nameA == nameB {
		t.Fatalf("distinct long databases must map to distinct locks, both %q", nameA)
	}
	if scopedMigrationLockName(longA) != nameA {
		t.Fatal("long lock name must be deterministic")
	}
}

// TestScopedMigrationLockNameInjective pins the collision fix: naive
// sanitization maps tenant-a and tenant_a to the same lock, so any name
// that sanitization would change must hash instead of sanitizing.
func TestScopedMigrationLockNameInjective(t *testing.T) {
	if scopedMigrationLockName("tenant-a") == scopedMigrationLockName("tenant_a") {
		t.Fatal("tenant-a and tenant_a must map to distinct locks")
	}
	if got := scopedMigrationLockName("tenant-a"); got == migrateLockName+"_tenant_a" {
		t.Fatalf("unsafe name must hash, got sanitized form %q", got)
	}
	for _, name := range []string{"my-db.v2/x", "db with spaces", strings.Repeat("a", 100)} {
		if got := scopedMigrationLockName(name); len(got) > maxMigrationLockLen {
			t.Fatalf("lock name %q exceeds %d characters", got, maxMigrationLockLen)
		}
	}
}

// TestMigrationDBKey pins the per-database in-process guard identity:
// different databases (or servers) map to distinct keys so their
// migrations never serialize on each other.
func TestMigrationDBKey(t *testing.T) {
	a := migrationDBKey("tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true&loc=UTC")
	b := migrationDBKey("tasuki:tasuki@tcp(localhost:3306)/other?parseTime=true&loc=UTC")
	if a == b {
		t.Fatal("different databases must have different migration keys")
	}
	c := migrationDBKey("tasuki:tasuki@tcp(127.0.0.1:3307)/tasuki?parseTime=true&loc=UTC")
	if a == c {
		t.Fatal("different servers must have different migration keys")
	}
	if d := migrationDBKey("tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true&loc=UTC"); d != a {
		t.Fatal("migration key must be deterministic")
	}
	if k := migrationDBKey("not a valid dsn %%%"); k == "" {
		t.Fatal("unparseable DSN needs a fallback key")
	}
}

// TestMigrationProcessLockPerDatabase pins the per-database in-process
// guard: unrelated databases proceed concurrently, the same database
// serializes, and a canceled context returns instead of blocking.
func TestMigrationProcessLockPerDatabase(t *testing.T) {
	ctx := context.Background()
	relA, err := acquireMigrationProcessLock(ctx, "test-migrate-db-a")
	if err != nil {
		t.Fatal(err)
	}
	// Unrelated database proceeds concurrently.
	relB, err := acquireMigrationProcessLock(ctx, "test-migrate-db-b")
	if err != nil {
		t.Fatal(err)
	}
	relB()
	// Same database blocks until released.
	acquired := make(chan func(), 1)
	go func() {
		rel, err := acquireMigrationProcessLock(context.Background(), "test-migrate-db-a")
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
	relC, err := acquireMigrationProcessLock(ctx, "test-migrate-db-c")
	if err != nil {
		t.Fatal(err)
	}
	defer relC()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireMigrationProcessLock(canceled, "test-migrate-db-c"); err == nil {
		t.Fatal("canceled context must not acquire the lock")
	}
}

// TestIsMissingIndexError pins the retryable purge-index rebuild (Codex
// round-21 P1 on #294): only MySQL error 1091 (missing index on DROP) may be
// tolerated for DROP INDEX statements. Every other failure (missing table
// 1146, permissions 1142, syntax 1064, duplicate key 1061) must abort the
// migration instead of being silently ignored.
func TestIsMissingIndexError(t *testing.T) {
	if isMissingIndexError(nil) {
		t.Fatal("nil is not a missing-index error")
	}
	if !isMissingIndexError(&driver.MySQLError{Number: 1091, Message: "Can't DROP 'wf_instances_completed_at_idx'; check that column/key exists"}) {
		t.Fatal("want error 1091 to be missing index")
	}
	for _, number := range []uint16{1146, 1142, 1064, 1061, 1060} {
		if isMissingIndexError(&driver.MySQLError{Number: number, Message: "other failure"}) {
			t.Fatalf("error %d must not be a missing-index error", number)
		}
	}
}

// TestIsDropIndex pins the DROP INDEX statement classifier: only DROP INDEX
// statements earn missing-index tolerance, so a stray tolerance can never
// mask a missing table/column elsewhere.
func TestIsDropIndex(t *testing.T) {
	if !isDropIndex(`ALTER TABLE wf_instances DROP INDEX wf_instances_completed_at_idx`) {
		t.Fatal("want DROP INDEX statement detected")
	}
	if !isDropIndex("alter table wf_instances drop index x") {
		t.Fatal("want case-insensitive DROP INDEX detection")
	}
	for _, stmt := range []string{
		`CREATE INDEX wf_instances_completed_at_idx ON wf_instances (completed_at, id)`,
		`ALTER TABLE wf_instances ADD COLUMN foo INT`,
		`DROP TABLE wf_instances`,
	} {
		if isDropIndex(stmt) {
			t.Fatalf("want %q to not be a DROP INDEX statement", stmt)
		}
	}
}
