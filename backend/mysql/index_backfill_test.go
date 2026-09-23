package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	driver "github.com/go-sql-driver/mysql"
)

type stubExecer struct {
	err error
}

func (s stubExecer) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, s.err
}

// The index backfill must ignore ONLY duplicate-index errors (1061): any
// other CREATE INDEX failure (bad column, privileges, engine limits) has to
// surface from Migrate instead of passing silently (Codex round 6 on #328).
func TestExecIndexBackfillSurfacesNonDuplicateFailures(t *testing.T) {
	const stmt = `CREATE INDEX wf_tasks_instance_idx ON wf_tasks (instance_id)`
	dupIndex := &driver.MySQLError{Number: 1061, Message: "Duplicate key name 'wf_tasks_instance_idx'"}
	cases := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{"success", nil, false},
		{"duplicate index ignored", dupIndex, false},
		{"wrapped duplicate index ignored", fmt.Errorf("exec: %w", dupIndex), false},
		{"duplicate column surfaces", &driver.MySQLError{Number: 1060, Message: "Duplicate column name"}, true},
		{"duplicate entry surfaces", &driver.MySQLError{Number: 1062, Message: "Duplicate entry"}, true},
		{"syntax error surfaces", &driver.MySQLError{Number: 1064, Message: "syntax error"}, true},
		{"generic error surfaces", errors.New("connection refused"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := execIndexBackfill(context.Background(), stubExecer{err: tc.err}, stmt)
			if tc.wantErr && err == nil {
				t.Fatalf("backfill with %v: want error, got nil (failure swallowed)", tc.err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("backfill with %v: want nil, got %v", tc.err, err)
			}
			if tc.wantErr && tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("backfill error %v does not wrap the driver error %v", err, tc.err)
			}
		})
	}
}
