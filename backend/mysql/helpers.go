package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func nowUTC() time.Time {
	return time.Now().UTC()
}

func jsonOrNull(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func isUniqueViolation(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func beginTx(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func commitConn(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "COMMIT")
	cerr := conn.Close()
	if err != nil {
		return err
	}
	return cerr
}

func rollbackConn(ctx context.Context, conn *sql.Conn) {
	_, _ = conn.ExecContext(ctx, "ROLLBACK")
	_ = conn.Close()
}

func withTx(ctx context.Context, db *sql.DB, fn func(conn *sql.Conn) error) error {
	conn, err := beginTx(ctx, db)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackConn(ctx, conn)
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if err := commitConn(ctx, conn); err != nil {
		return err
	}
	committed = true
	return nil
}

func inClause(n int) string {
	if n <= 0 {
		return "('')"
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

type activityPayload struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
	Retry retryJSON       `json:"retry"`
}

type retryJSON struct {
	InitialIntervalMs  int64   `json:"initial_interval_ms"`
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	MaxIntervalMs      int64   `json:"max_interval_ms"`
	MaxAttempts        int     `json:"max_attempts"`
}

type inboxEnv struct {
	Name string          `json:"_name,omitempty"`
	Body json.RawMessage `json:"_body,omitempty"`
}

func inboxPayload(ev journal.Event) []byte {
	if ev.Name == "" {
		return ev.Payload
	}
	b, _ := json.Marshal(inboxEnv{Name: ev.Name, Body: ev.Payload})
	return b
}

func unwrapInboxPayload(payload []byte) (string, []byte) {
	if len(payload) == 0 {
		return "", nil
	}
	var env inboxEnv
	if err := json.Unmarshal(payload, &env); err == nil && env.Name != "" {
		return env.Name, []byte(env.Body)
	}
	return "", payload
}

func enqueueWorkflowTask(ctx context.Context, q queryExecer, instanceID string) error {
	now := nowUTC()
	_, err := q.ExecContext(ctx, `
		INSERT IGNORE INTO wf_tasks (kind, instance_id, queue, visible_at, created_at)
		SELECT 'workflow', ?, queue, ?, ? FROM wf_instances WHERE id = ? AND status = 'running'`,
		instanceID, now, now, instanceID)
	return err
}

func ensureWorkflowTaskIfInbox(ctx context.Context, q queryExecer, instanceID string) error {
	now := nowUTC()
	_, err := q.ExecContext(ctx, `
		INSERT IGNORE INTO wf_tasks (kind, instance_id, queue, visible_at, created_at)
		SELECT 'workflow', i.id, i.queue, ?, ?
		FROM wf_instances i
		WHERE i.id = ? AND i.status = 'running'
		  AND EXISTS (SELECT 1 FROM wf_inbox WHERE instance_id = ?)`,
		now, now, instanceID, instanceID)
	return err
}

type queryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func nullIfZeroRefSeq(refSeq int64) any {
	if refSeq == 0 {
		return nil
	}
	return refSeq
}

func scanNullableInt64(v sql.NullInt64) int64 {
	if v.Valid {
		return v.Int64
	}
	return 0
}

func scanJSONNullString(v sql.NullString) []byte {
	if !v.Valid || v.String == "" {
		return nil
	}
	return []byte(v.String)
}

func scanSearchAttrs(v sql.NullString) map[string]string {
	m, _ := backend.SearchAttributesFromPayload(scanJSONNullString(v))
	return m
}

func leaseVisibleAt(lease time.Duration) time.Time {
	return nowUTC().Add(lease)
}
