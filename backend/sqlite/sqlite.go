package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/backend/hub"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaFS embed.FS

// Backend is the SQLite implementation of backend.Backend.
type Backend struct {
	db  *sql.DB
	hub *hub.Hub
}

// New opens a SQLite database at path (use ":memory:" for ephemeral).
func New(path string) (*Backend, error) {
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	} else {
		dsn = "file:tasuki?mode=memory&cache=shared&_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	db.SetMaxOpenConns(1) // single-writer
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}
	return &Backend{db: db, hub: hub.New()}, nil
}

func (b *Backend) Close() error {
	return b.db.Close()
}

func (b *Backend) Migrate(ctx context.Context) error {
	sqlBytes, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	if _, err = b.db.ExecContext(ctx, string(sqlBytes)); err != nil {
		return err
	}
	_, _ = b.db.ExecContext(ctx, `ALTER TABLE wf_tasks ADD COLUMN heartbeat TEXT`)
	_, _ = b.db.ExecContext(ctx, `ALTER TABLE wf_instances ADD COLUMN search_attributes TEXT NOT NULL DEFAULT '{}'`)
	return nil
}

// Reset truncates all tables (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	_, err := b.db.ExecContext(ctx, `
		DELETE FROM wf_schedules;
		DELETE FROM wf_timers;
		DELETE FROM wf_tasks;
		DELETE FROM wf_inbox;
		DELETE FROM wf_signal_dedupe;
		DELETE FROM wf_journal;
		DELETE FROM wf_instances;
		DELETE FROM sqlite_sequence WHERE name IN ('wf_tasks','wf_inbox');
	`)
	return err
}

func (b *Backend) DB() *sql.DB { return b.db }
