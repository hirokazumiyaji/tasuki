package postgres

import (
	"context"
	"embed"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationTableName tracks applied migration versions. It is deliberately
// distinct from the tables used by golang-migrate / goose so the two systems
// never fight over bookkeeping.
const migrationTableName = "tasuki_schema_migrations"

type migration struct {
	version int64
	name    string
	up      string
}

var (
	embeddedMigrations []migration
	embedErr           error
	migrationsOnce     sync.Once
)

func loadMigrations() ([]migration, error) {
	migrationsOnce.Do(func() {
		entries, err := migrationsFS.ReadDir("migrations")
		if err != nil {
			embedErr = fmt.Errorf("postgres: read migrations: %w", err)
			return
		}
		byVersion := map[int64]*migration{}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".up.sql") {
				continue
			}
			base := strings.TrimSuffix(name, ".up.sql")
			idx := strings.IndexByte(base, '_')
			if idx <= 0 {
				embedErr = fmt.Errorf("postgres: migration %s: want <6-digit-version>_<name>.up.sql", name)
				return
			}
			version, err := strconv.ParseInt(base[:idx], 10, 64)
			if err != nil || version <= 0 {
				embedErr = fmt.Errorf("postgres: migration %s: bad version", name)
				return
			}
			if _, dup := byVersion[version]; dup {
				embedErr = fmt.Errorf("postgres: duplicate migration version %d", version)
				return
			}
			sql, err := migrationsFS.ReadFile(path.Join("migrations", name))
			if err != nil {
				embedErr = err
				return
			}
			byVersion[version] = &migration{version: version, name: base[idx+1:], up: string(sql)}
		}
		for _, m := range byVersion {
			embeddedMigrations = append(embeddedMigrations, *m)
		}
		sort.Slice(embeddedMigrations, func(i, j int) bool {
			return embeddedMigrations[i].version < embeddedMigrations[j].version
		})
		if len(embeddedMigrations) == 0 {
			embedErr = fmt.Errorf("postgres: no migrations embedded")
		}
	})
	return embeddedMigrations, embedErr
}

// LatestSchemaVersion reports the highest migration version embedded in this build.
func LatestSchemaVersion() (int64, error) {
	migs, err := loadMigrations()
	if err != nil {
		return 0, err
	}
	return migs[len(migs)-1].version, nil
}

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

// Migrate applies pending migrations in order, recording each version in
// tasuki_schema_migrations. It is safe to call concurrently and repeatedly.
//
// Databases created before versioned migrations (via the old cumulative
// schema.sql) are detected and stamped at version 1 without re-running DDL,
// then upgraded by any newer migrations.
func (b *Backend) Migrate(ctx context.Context) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if _, err := b.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+migrationTableName+` (
			version    bigint PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("postgres: create %s: %w", migrationTableName, err)
	}

	// Legacy databases have the schema but no version bookkeeping. Their
	// content matches the old cumulative schema.sql, which is equivalent to
	// version 1; stamp it instead of re-running DDL.
	var applied int64
	if err := b.pool.QueryRow(ctx,
		`SELECT count(*) FROM `+migrationTableName).Scan(&applied); err != nil {
		return err
	}
	if applied == 0 {
		var exists bool
		if err := b.pool.QueryRow(ctx,
			`SELECT to_regclass('wf_instances') IS NOT NULL`).Scan(&exists); err != nil {
			return err
		}
		if exists {
			if _, err := b.pool.Exec(ctx,
				`INSERT INTO `+migrationTableName+` (version) VALUES (1) ON CONFLICT DO NOTHING`); err != nil {
				return err
			}
		}
	}

	for _, m := range migs {
		if err := b.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("postgres: migration %06d_%s: %w", m.version, m.name, err)
		}
	}
	return nil
}

// applyMigration claims the version first (atomic via PK conflict), then runs
// the DDL in the same transaction as the bookkeeping insert.
func (b *Backend) applyMigration(ctx context.Context, m migration) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`INSERT INTO `+migrationTableName+` (version) VALUES ($1) ON CONFLICT DO NOTHING`, m.version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // already applied
	}
	if _, err := tx.Exec(ctx, m.up); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SchemaVersion returns the highest applied migration version, or 0 when the
// database has never been migrated (or predates version tracking).
func (b *Backend) SchemaVersion(ctx context.Context) (int64, error) {
	var exists bool
	if err := b.pool.QueryRow(ctx,
		`SELECT to_regclass('`+migrationTableName+`') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var version int64
	if err := b.pool.QueryRow(ctx,
		`SELECT COALESCE(max(version), 0) FROM `+migrationTableName).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

// requiredTables must exist before the Backend can serve traffic. ValidateSchema
// fails fast when migrations have not run (or were rolled back), so callers can
// refuse to start instead of erroring on every operation.
var requiredTables = []string{
	"wf_instances", "wf_journal", "wf_inbox", "wf_signal_dedupe",
	"wf_tasks", "wf_timers", "wf_schedules",
}

// ValidateSchema implements backend.SchemaValidator. It checks that every
// required table resolves through the connection's search_path; missing tables
// are reported together in one error.
func (b *Backend) ValidateSchema(ctx context.Context) error {
	var missing []string
	for _, t := range requiredTables {
		var exists bool
		if err := b.pool.QueryRow(ctx,
			`SELECT to_regclass($1) IS NOT NULL`, t).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: validate schema: %w", err)
		}
		if !exists {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("postgres: schema is missing tables %s; run Migrate (or your migration tool) first",
			strings.Join(missing, ", "))
	}
	return nil
}
