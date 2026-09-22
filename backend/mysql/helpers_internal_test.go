package mysql

import (
	"strings"
	"testing"

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
