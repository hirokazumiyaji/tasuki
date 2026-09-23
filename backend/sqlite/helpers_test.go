package sqlite

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestParseTime_Formats(t *testing.T) {
	fixed := "2026-01-02T03:04:05.000000000Z"
	got, err := parseTime(fixed)
	if err != nil || !got.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("%v %v", got, err)
	}
	got, err = parseTime("2026-01-02T03:04:05Z")
	if err != nil {
		t.Fatal(err)
	}
	if got.Year() != 2026 {
		t.Fatal(got)
	}
	if _, err := parseTime("not-a-time"); err == nil {
		t.Fatal("want error")
	}
}

func TestIsDuplicateColumnError(t *testing.T) {
	if isDuplicateColumnError(nil) {
		t.Fatal("nil is not a duplicate-column error")
	}
	if !isDuplicateColumnError(errors.New("duplicate column name: heartbeat")) {
		t.Fatal("want duplicate column")
	}
	// Any other ALTER failure (missing table, permissions, syntax) must
	// NOT be classified as duplicate-column: Migrate aborts on those
	// instead of silently ignoring them.
	for _, msg := range []string{
		"no such table: wf_tasks",
		"permission denied",
		"near \"ADD\": syntax error",
	} {
		if isDuplicateColumnError(errors.New(msg)) {
			t.Fatalf("%q must not be a duplicate-column error", msg)
		}
	}
}

func TestIsUniqueViolationAndScanners(t *testing.T) {
	if isUniqueViolation(nil) || isUniqueViolation(errors.New("other")) {
		t.Fatal("false cases")
	}
	if !isUniqueViolation(errors.New("UNIQUE constraint failed: wf_instances.id")) {
		t.Fatal("want unique")
	}
	if scanNullableString(sql.NullString{}) != "" {
		t.Fatal("empty")
	}
	if scanNullableString(sql.NullString{String: "x", Valid: true}) != "x" {
		t.Fatal("valid")
	}
	if scanNullableInt64(sql.NullInt64{}) != 0 || scanNullableInt64(sql.NullInt64{Int64: 7, Valid: true}) != 7 {
		t.Fatal("int")
	}
}
