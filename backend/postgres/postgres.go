package postgres

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaFS embed.FS

// Backend is the PostgreSQL implementation of backend.Backend.
type Backend struct {
	pool *pgxpool.Pool
}

// New opens a connection pool to PostgreSQL.
func New(ctx context.Context, dsn string) (*Backend, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Backend{pool: pool}, nil
}

func (b *Backend) Close() {
	b.pool.Close()
}

// Pool exposes the underlying connection pool. Use it for monitoring queries
// (pg_stat_user_tables, pgstattuple) and operational maintenance.
func (b *Backend) Pool() *pgxpool.Pool {
	return b.pool
}

func (b *Backend) Migrate(ctx context.Context) error {
	sql, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	_, err = b.pool.Exec(ctx, string(sql))
	return err
}
