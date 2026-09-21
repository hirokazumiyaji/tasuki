package mysql

import (
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
