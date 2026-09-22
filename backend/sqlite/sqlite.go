package sqlite

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
	_ "modernc.org/sqlite"
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
			embedErr = fmt.Errorf("sqlite: read migrations: %w", err)
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
				embedErr = fmt.Errorf("sqlite: migration %s: want <6-digit-version>_<name>.up.sql", name)
				return
			}
			version, err := strconv.ParseInt(base[:idx], 10, 64)
			if err != nil || version <= 0 {
				embedErr = fmt.Errorf("sqlite: migration %s: bad version", name)
				return
			}
			if _, dup := byVersion[version]; dup {
				embedErr = fmt.Errorf("sqlite: duplicate migration version %d", version)
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
			embedErr = fmt.Errorf("sqlite: no migrations embedded")
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

// Backend is the SQLite implementation of backend.Backend.
type Backend struct {
	db  *sql.DB
	hub *hub.Hub
	// migrateKey identifies the target database file for the in-process
	// migration guard (see acquireMigrationProcessLock): the normalized
	// DSN, so two handles on the same file (including the shared-cache
	// ":memory:" database) serialize while unrelated files migrate
	// concurrently.
	migrateKey string
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
	return &Backend{db: db, hub: hub.New(), migrateKey: "sqlite://" + dsn}, nil
}

func (b *Backend) Close() error {
	return b.db.Close()
}

// migrationProcessLocks keys in-process migration guards by database
// identity (the normalized DSN) so migrations for unrelated database files
// proceed concurrently while migrations for the same file never interleave
// migration steps. SQLite has no advisory lock, so for the same file this
// per-database guard plus the BEGIN IMMEDIATE write transaction (which
// serializes cross-process writers) is the whole exclusion.
var (
	migrationProcessLocksMu sync.Mutex
	migrationProcessLocks   = map[string]*migrationProcessLock{}
)

// migrationProcessLock is a refcounted binary semaphore. The refcount lets
// the registry drop entries once no goroutine references the key, so the map
// does not grow with the number of distinct databases seen.
type migrationProcessLock struct {
	sem  chan struct{}
	refs int
}

// acquireMigrationProcessLock holds the in-process migration guard for key.
// Unlike sync.Mutex.Lock it honors ctx: a canceled context returns ctx.Err()
// instead of blocking forever behind a stuck migration. The returned release
// must be called exactly once after a successful acquisition.
func acquireMigrationProcessLock(ctx context.Context, key string) (release func(), err error) {
	migrationProcessLocksMu.Lock()
	l, ok := migrationProcessLocks[key]
	if !ok {
		l = &migrationProcessLock{sem: make(chan struct{}, 1)}
		l.sem <- struct{}{}
		migrationProcessLocks[key] = l
	}
	l.refs++
	migrationProcessLocksMu.Unlock()
	select {
	case <-ctx.Done():
		migrationProcessLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(migrationProcessLocks, key)
		}
		migrationProcessLocksMu.Unlock()
		return nil, ctx.Err()
	case <-l.sem:
	}
	return func() {
		l.sem <- struct{}{}
		migrationProcessLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(migrationProcessLocks, key)
		}
		migrationProcessLocksMu.Unlock()
	}, nil
}

// Migrate applies pending migrations in order, recording each version in
// tasuki_schema_migrations. It is safe to call repeatedly.
//
// Databases created before versioned migrations (via the old cumulative
// schema.sql) are detected and stamped at version 1 without re-running DDL,
// then upgraded by any newer migrations. The stamp applies only when the
// complete baseline is present (see baselineComplete): a partial legacy
// schema runs the idempotent baseline DDL instead of skipping it.
//
// Concurrency: each migration runs inside a single BEGIN IMMEDIATE
// transaction that checks the version, runs the DDL, and records the version
// before committing, so a concurrent Migrate never observes a version whose
// schema is still incomplete. A failed migration rolls its transaction back
// (with a non-canceled context, so cancellation cannot leave the version
// marked as applied) and the next Migrate retries the DDL.
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
	migrateRelease, err := acquireMigrationProcessLock(ctx, b.migrateKey)
	if err != nil {
		return err
	}
	defer migrateRelease()
	if _, err := b.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+migrationTableName+` (
			version    INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		)`); err != nil {
		return fmt.Errorf("sqlite: create %s: %w", migrationTableName, err)
	}

	// Legacy databases have the schema but no version bookkeeping. Stamp
	// version 1 only when the complete baseline is present. A database
	// from before wf_signal_dedupe (or any other baseline table) was
	// introduced has wf_instances but is missing tables; stamping it
	// would skip the baseline DDL that creates them (migration 2 only
	// adds columns), leaving ValidateSchema to reject the database at
	// startup. An incomplete baseline falls through to the normal path,
	// whose idempotent CREATE TABLE IF NOT EXISTS repairs it.
	var applied int64
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationTableName).Scan(&applied); err != nil {
		return fmt.Errorf("sqlite: read %s: %w", migrationTableName, err)
	}
	if applied == 0 {
		complete, err := baselineComplete(ctx, b.db)
		if err != nil {
			return err
		}
		if complete {
			if _, err := b.db.ExecContext(ctx,
				`INSERT OR IGNORE INTO `+migrationTableName+` (version) VALUES (1)`); err != nil {
				return fmt.Errorf("sqlite: stamp legacy schema: %w", err)
			}
		}
	}

	for _, m := range migs {
		if err := b.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("sqlite: migration %06d_%s: %w", m.version, m.name, err)
		}
	}
	return nil
}

// baselineComplete reports whether every table created by the baseline
// migration exists. Only then may a legacy database without version
// bookkeeping be stamped as version 1.
func baselineComplete(ctx context.Context, db *sql.DB) (bool, error) {
	for _, t := range requiredTables {
		exists, err := tableExists(ctx, db, t)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// applyMigration runs the migration inside one write transaction: it checks
// whether the version is already applied, runs the DDL, and records the
// version before committing. The version therefore becomes visible only
// together with its completed schema, and a failure (or cancellation) rolls
// everything back so the next Migrate retries instead of skipping.
func (b *Backend) applyMigration(ctx context.Context, m migration) error {
	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return err
	}
	// Roll back with a non-canceled context so a canceled caller cannot
	// leave the write transaction (or its lock) behind.
	cleanup := context.WithoutCancel(ctx)
	committed := false
	defer func() {
		if !committed {
			rollbackConn(cleanup, conn)
		}
	}()
	var applied int64
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationTableName+` WHERE version = ?`, m.version).Scan(&applied); err != nil {
		return err
	}
	if applied > 0 {
		if err := commitConn(cleanup, conn); err != nil {
			return err
		}
		committed = true
		return nil // already applied
	}
	for _, stmt := range splitSQL(m.up) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			if isAddColumn(stmt) && isDuplicateColumnError(err) {
				continue // already backfilled; keep going
			}
			return fmt.Errorf("sqlite migrate: %w\nstmt: %s", err, stmt)
		}
	}
	if _, err := conn.ExecContext(cleanup,
		`INSERT INTO `+migrationTableName+` (version) VALUES (?)`, m.version); err != nil {
		if isUniqueViolation(err) {
			return nil // another migrator completed the same DDL first
		}
		return err
	}
	if err := commitConn(cleanup, conn); err != nil {
		return err
	}
	committed = true
	return nil
}

func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlite: check table %s: %w", table, err)
	}
	return true, nil
}

// isAddColumn reports whether stmt adds a column. Only such statements may
// have their errors tolerated (and then only duplicate-column ones); every
// other statement failure is fatal.
func isAddColumn(stmt string) bool {
	return strings.Contains(strings.ToUpper(stmt), "ADD COLUMN")
}

// isDuplicateColumnError reports whether err is a duplicate-column error.
// Only this class of ALTER TABLE ... ADD COLUMN failure may be tolerated by
// Migrate; everything else (permissions, missing table, syntax) must abort
// the migration.
func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column")
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
		return 0, fmt.Errorf("sqlite: read %s: %w", migrationTableName, err)
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

// requiredColumns are columns added by post-baseline migrations. A database
// whose tables all exist but which lacks one of these columns predates the
// backfill migration: ordinary reads and writes would fail, so validation
// must fail too instead of reporting a healthy schema.
var requiredColumns = [][2]string{
	{"wf_tasks", "heartbeat"},
	{"wf_instances", "search_attributes"},
	{"wf_instances", "memo"},
}

// ValidateSchema implements backend.SchemaValidator. It checks that every
// required table exists and that every required column is present; missing
// tables or columns are reported together in one error.
func (b *Backend) ValidateSchema(ctx context.Context) error {
	args := make([]any, 0, len(requiredTables))
	for _, t := range requiredTables {
		args = append(args, t)
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (`+inClause(len(requiredTables))+`)`,
		args...)
	if err != nil {
		return fmt.Errorf("sqlite: validate schema: %w", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("sqlite: validate schema: %w", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: validate schema: %w", err)
	}
	var missing []string
	for _, t := range requiredTables {
		if !found[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("sqlite: schema is missing tables %s; run Migrate (or your migration tool) first",
			strings.Join(missing, ", "))
	}
	var missingCols []string
	for _, c := range requiredColumns {
		var name string
		err := b.db.QueryRowContext(ctx,
			`SELECT name FROM pragma_table_info(?) WHERE name = ?`, c[0], c[1]).Scan(&name)
		if err == sql.ErrNoRows {
			missingCols = append(missingCols, c[0]+"."+c[1])
			continue
		}
		if err != nil {
			return fmt.Errorf("sqlite: validate schema: %w", err)
		}
	}
	if len(missingCols) > 0 {
		return fmt.Errorf("sqlite: schema is missing columns %s; run Migrate (or your migration tool) first",
			strings.Join(missingCols, ", "))
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
