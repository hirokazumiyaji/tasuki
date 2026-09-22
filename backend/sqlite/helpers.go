package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func nowUTC() time.Time {
	return time.Now().UTC()
}

func formatTime(t time.Time) string {
	// Fixed 9-digit fractional seconds so lexicographic compare matches time order.
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse("2006-01-02T15:04:05.000000000Z", s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return time.Parse(time.RFC3339, s)
		}
	}
	return t, nil
}

func nowStr() string {
	return formatTime(nowUTC())
}

func jsonOrNull(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func beginImmediate(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
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
	conn, err := beginImmediate(ctx, db)
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

// sortedSearchAttributeKeys returns the filter keys in sorted order so the
// generated SQL is deterministic.
func sortedSearchAttributeKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type activityPayload struct {
	Name                    string          `json:"name"`
	Input                   json.RawMessage `json:"input"`
	Retry                   retryJSON       `json:"retry"`
	StartToCloseTimeoutMs   int64           `json:"start_to_close_timeout_ms,omitempty"`
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
	_, err := q.ExecContext(ctx, `
		INSERT OR IGNORE INTO wf_tasks (kind, instance_id, queue, visible_at, created_at)
		SELECT 'workflow', ?, queue, ?, ? FROM wf_instances WHERE id = ? AND status = 'running'`,
		instanceID, nowStr(), nowStr(), instanceID)
	return err
}

func ensureWorkflowTaskIfInbox(ctx context.Context, q queryExecer, instanceID string) error {
	_, err := q.ExecContext(ctx, `
		INSERT OR IGNORE INTO wf_tasks (kind, instance_id, queue, visible_at, created_at)
		SELECT 'workflow', i.id, i.queue, ?, ?
		FROM wf_instances i
		WHERE i.id = ? AND i.status = 'running'
		  AND EXISTS (SELECT 1 FROM wf_inbox WHERE instance_id = ?)`,
		nowStr(), nowStr(), instanceID, instanceID)
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

func scanNullableString(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return ""
}

func scanJSONText(v sql.NullString) []byte {
	if !v.Valid || v.String == "" {
		return nil
	}
	return []byte(v.String)
}

func scanSearchAttrs(v sql.NullString) map[string]string {
	m, _ := backend.SearchAttributesFromPayload(scanJSONText(v))
	return m
}

func leaseVisibleAt(lease time.Duration) string {
	return formatTime(nowUTC().Add(lease))
}
