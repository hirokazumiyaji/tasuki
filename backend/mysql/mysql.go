package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hirokazumiyaji/tasuki/backend/hub"
	_ "github.com/go-sql-driver/mysql"
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
			embedErr = fmt.Errorf("mysql: read migrations: %w", err)
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
				embedErr = fmt.Errorf("mysql: migration %s: want <6-digit-version>_<name>.up.sql", name)
				return
			}
			version, err := strconv.ParseInt(base[:idx], 10, 64)
			if err != nil || version <= 0 {
				embedErr = fmt.Errorf("mysql: migration %s: bad version", name)
				return
			}
			if _, dup := byVersion[version]; dup {
				embedErr = fmt.Errorf("mysql: duplicate migration version %d", version)
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
			embedErr = fmt.Errorf("mysql: no migrations embedded")
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

// Migrate applies pending migrations in order, recording each version in
// tasuki_schema_migrations. It is safe to call repeatedly.
//
// Databases created before versioned migrations (via the old cumulative
// schema.sql) are detected and stamped at version 1 without re-running DDL,
// then upgraded by any newer migrations.
//
// Column-backfill ALTERs tolerate duplicate-column errors (the column is
// already there, e.g. the database was created by a newer schema) but every
// other failure - permission denied, missing table, syntax errors - aborts
// the migration with an error instead of being silently ignored.
func (b *Backend) Migrate(ctx context.Context) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}
	if _, err := b.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+migrationTableName+` (
			version    BIGINT PRIMARY KEY,
			applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		)`); err != nil {
		return fmt.Errorf("mysql: create %s: %w", migrationTableName, err)
	}

	// Legacy databases have the schema but no version bookkeeping. Their
	// content matches the old cumulative schema.sql, which is equivalent to
	// version 1; stamp it instead of re-running DDL.
	var applied int64
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationTableName).Scan(&applied); err != nil {
		return fmt.Errorf("mysql: read %s: %w", migrationTableName, err)
	}
	if applied == 0 {
		exists, err := tableExists(ctx, b.db, "wf_instances")
		if err != nil {
			return err
		}
		if exists {
			if _, err := b.db.ExecContext(ctx,
				`INSERT IGNORE INTO `+migrationTableName+` (version) VALUES (1)`); err != nil {
				return fmt.Errorf("mysql: stamp legacy schema: %w", err)
			}
		}
	}

	for _, m := range migs {
		if err := b.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("mysql: migration %06d_%s: %w", m.version, m.name, err)
		}
	}
	return nil
}

// applyMigration claims the version first (atomic via PK conflict with
// INSERT IGNORE), then runs the DDL. MySQL DDL commits implicitly, so a
// failed migration drops its claim row (best effort): without this the
// failure would look applied and never be retried.
func (b *Backend) applyMigration(ctx context.Context, m migration) error {
	res, err := b.db.ExecContext(ctx,
		`INSERT IGNORE INTO `+migrationTableName+` (version) VALUES (?)`, m.version)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil // already applied
	}
	for _, stmt := range splitSQL(m.up) {
		if _, err := b.db.ExecContext(ctx, stmt); err != nil {
			if isAddColumn(stmt) && isDuplicateColumnError(err) {
				continue // already backfilled; keep going
			}
			_, _ = b.db.ExecContext(ctx,
				`DELETE FROM `+migrationTableName+` WHERE version = ?`, m.version)
			return fmt.Errorf("mysql migrate: %w\nstmt: %s", err, stmt)
		}
	}
	return nil
}

func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var n int64
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
		table).Scan(&n); err != nil {
		return false, fmt.Errorf("mysql: check table %s: %w", table, err)
	}
	return n > 0, nil
}

// isAddColumn reports whether stmt adds a column. Only such statements may
// have their errors tolerated (and then only duplicate-column ones); every
// other statement failure is fatal.
func isAddColumn(stmt string) bool {
	return strings.Contains(strings.ToUpper(stmt), "ADD COLUMN")
}

// SchemaVersion returns the highest applied migration version, or 0 when the
// database has never been migrated (or predates version tracking).
func (b *Backend) SchemaVersion(ctx context.Context) (int64, error) {
	exists, err := tableExists(ctx, b.db, migrationTableName)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var version int64
	if err := b.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM `+migrationTableName).Scan(&version); err != nil {
		return 0, fmt.Errorf("mysql: read %s: %w", migrationTableName, err)
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
// required table exists in the current database; missing tables are reported
// together in one error.
func (b *Backend) ValidateSchema(ctx context.Context) error {
	args := make([]any, 0, len(requiredTables))
	for _, t := range requiredTables {
		args = append(args, t)
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN (`+inClause(len(requiredTables))+`)`,
		args...)
	if err != nil {
		return fmt.Errorf("mysql: validate schema: %w", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("mysql: validate schema: %w", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: validate schema: %w", err)
	}
	var missing []string
	for _, t := range requiredTables {
		if !found[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("mysql: schema is missing tables %s; run Migrate (or your migration tool) first",
			strings.Join(missing, ", "))
	}
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
