package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"github.com/hirokazumiyaji/tasuki/backend/hub"
	_ "github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schemaFS embed.FS

// Backend is the MySQL / MariaDB implementation of backend.Backend.
type Backend struct {
	db  *sql.DB
	hub *hub.Hub
}

// New opens a connection pool. dsn example:
//
//	tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true&loc=UTC
func New(ctx context.Context, dsn string) (*Backend, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	return &Backend{db: db, hub: hub.New()}, nil
}

func (b *Backend) Close() error {
	return b.db.Close()
}

func (b *Backend) DB() *sql.DB { return b.db }

func (b *Backend) Migrate(ctx context.Context) error {
	sqlBytes, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	for _, stmt := range splitSQL(string(sqlBytes)) {
		if _, err := b.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql migrate: %w\nstmt: %s", err, stmt)
		}
	}
	_, _ = b.db.ExecContext(ctx, `ALTER TABLE wf_tasks ADD COLUMN heartbeat BLOB NULL`)
	return nil
}

// Reset truncates all tables (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	stmts := []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"TRUNCATE TABLE wf_schedules",
		"TRUNCATE TABLE wf_timers",
		"TRUNCATE TABLE wf_tasks",
		"TRUNCATE TABLE wf_inbox",
		"TRUNCATE TABLE wf_signal_dedupe",
		"TRUNCATE TABLE wf_journal",
		"TRUNCATE TABLE wf_instances",
		"SET FOREIGN_KEY_CHECKS = 1",
	}
	for _, stmt := range stmts {
		if _, err := b.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func splitSQL(s string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(s, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(trim, ";") {
			stmt := strings.TrimSpace(cur.String())
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				out = append(out, stmt)
			}
			cur.Reset()
		}
	}
	if stmt := strings.TrimSpace(cur.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out
}
