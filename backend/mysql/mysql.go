package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	driver "github.com/go-sql-driver/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
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
	// migrateKey identifies the target database for the in-process
	// migration guard (see acquireMigrationProcessLock): server address +
	// database name parsed from the DSN at New time.
	migrateKey string
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
	return &Backend{db: db, hub: hub.New(), migrateKey: migrationDBKey(dsn)}, nil
}

func (b *Backend) Close() error {
	return b.db.Close()
}

func (b *Backend) DB() *sql.DB { return b.db }

// migrationProcessLocks keys in-process migration guards by database
// identity so migrations for unrelated databases proceed concurrently while
// migrations for the same database never interleave migration steps. The
// cross-process exclusion still comes from the per-database MySQL named lock
// (see scopedMigrationLockName), held for the whole run, except on TiDB
// where GET_LOCK is unavailable and the named-lock step is skipped (see
// isTiDB); the in-process guard only serializes goroutines sharing this process.
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

// migrationDBKey derives the in-process migration guard key from the DSN:
// server address plus selected database. Unrelated databases (different
// server or different database name) map to distinct keys; an unparseable
// DSN falls back to a single global key (over-serializing, which is always
// safe, instead of risking concurrent migrations on the same database).
func migrationDBKey(dsn string) string {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return "mysql-default"
	}
	return "mysql://" + cfg.Addr + "/" + cfg.DBName
}

// migrateLockName is the base MySQL named lock (GET_LOCK/RELEASE_LOCK)
// guarding cross-process migrations. The effective lock is scoped to the
// selected database (see scopedMigrationLockName); the bare base is used
// only when no default database is selected.
const migrateLockName = "tasuki_migrate_lock"

// maxMigrationLockLen is MySQL's limit for GET_LOCK names.
const maxMigrationLockLen = 64

// isLockNameSafe reports whether s uses only the MySQL GET_LOCK-safe
// alphabet, i.e. scopedMigrationLockName can embed it verbatim. Any other
// string must be hashed: naive sanitization maps distinct names (tenant-a
// vs tenant_a) to the same lock.
func isLockNameSafe(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '$':
		default:
			return false
		}
	}
	return true
}

// scopedMigrationLockName scopes the migration lock to one database so
// migrations for unrelated databases on the same server never block each
// other. The mapping is injective: the original name is used verbatim when
// it is already lock-safe and fits MySQL's 64-character GET_LOCK limit;
// otherwise (sanitization would change the string, or the name overflows)
// the lock is base + "_" + hex(sha256(dbName)) truncated to fit, so
// tenant-a and tenant_a (or distinct overlong names) never share a lock.
func scopedMigrationLockName(dbName string) string {
	if dbName == "" {
		return migrateLockName
	}
	if isLockNameSafe(dbName) {
		if name := migrateLockName + "_" + dbName; len(name) <= maxMigrationLockLen {
			return name
		}
	}
	// Hash whenever sanitization would change the string or the length
	// overflows. The 64-hex digest is truncated to 16 characters (64 bits):
	// base (18) + "_" + digest (16) = 35 characters, well under the limit.
	sum := sha256.Sum256([]byte(dbName))
	return migrateLockName + "_" + hex.EncodeToString(sum[:])[:16]
}

// migrationQueryer is the statement surface Migrate needs. It is satisfied
// by both *sql.DB and *sql.Conn; Migrate passes the lock-holding *sql.Conn
// so bookkeeping and DDL never wait on the pool while the named lock's
// session occupies its only slot (see P2: MaxOpenConns(1) deadlock).
type migrationQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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
// Concurrency: Migrate holds the per-database in-process guard (keyed by
// server + database, ctx-aware) and a per-database MySQL
// named lock (cross-process) for the whole run, so a concurrent Migrate
// blocks instead of observing a half-applied schema. On TiDB the named-lock
// step is skipped (TiDB has no GET_LOCK/RELEASE_LOCK; see isTiDB): the
// in-process guard still serializes goroutines in this process, and the DDL
// itself is idempotent (CREATE TABLE IF NOT EXISTS, INSERT IGNORE,
// duplicate-column tolerance), so concurrent Migrates from separate
// processes may interleave but converge instead of corrupting the schema.
// Cross-process exclusion on TiDB is therefore weaker than on MySQL. Every statement runs
// on lockConn, the
// session holding the named lock: with MaxOpenConns(1) that session occupies
// the pool's only connection, so touching b.db here would wait for a
// connection that cannot free up until Migrate returns. Each version row is
// inserted only after its DDL has completed, so a failure leaves no version
// row behind and a retry simply re-runs the idempotent DDL - there is no
// claim row to clean up, and no cleanup that could reuse a canceled context.
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
	releaseProcessLock, err := acquireMigrationProcessLock(ctx, b.migrateKey)
	if err != nil {
		return err
	}
	defer releaseProcessLock()

	// Hold a per-database named lock for the whole migration. A concurrent
	// Migrate on another host targeting the same database blocks in GET_LOCK
	// instead of proceeding against a half-migrated schema, while migrations
	// for unrelated databases on the same server never contend. The lock
	// lives on lockConn, which is kept open until Migrate returns, and every
	// statement below runs on lockConn: it is the session holding the named
	// lock, and with MaxOpenConns(1) no other connection can be checked out
	// until Migrate returns.
	//
	// TiDB has no GET_LOCK/RELEASE_LOCK, so the named-lock step is skipped
	// there (see isTiDB): the in-process guard above plus the idempotent DDL
	// below are the only exclusion, and concurrent cross-process Migrates
	// may interleave but converge.
	lockConn, err := b.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: acquire migration lock: %w", err)
	}
	defer lockConn.Close()
	tidb, err := isTiDB(ctx, lockConn)
	if err != nil {
		return err
	}
	if !tidb {
		lockName, err := acquireMigrationLock(ctx, lockConn)
		if err != nil {
			return err
		}
		defer releaseMigrationLock(context.WithoutCancel(ctx), lockConn, lockName)
	}
	if _, err := lockConn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+migrationTableName+` (
			version    BIGINT PRIMARY KEY,
			applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		)`); err != nil {
		return fmt.Errorf("mysql: create %s: %w", migrationTableName, err)
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
	if err := lockConn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationTableName).Scan(&applied); err != nil {
		return fmt.Errorf("mysql: read %s: %w", migrationTableName, err)
	}
	if applied == 0 {
		complete, err := baselineComplete(ctx, lockConn)
		if err != nil {
			return err
		}
		if complete {
			if _, err := lockConn.ExecContext(ctx,
				`INSERT IGNORE INTO `+migrationTableName+` (version) VALUES (1)`); err != nil {
				return fmt.Errorf("mysql: stamp legacy schema: %w", err)
			}
		}
	}

	for _, m := range migs {
		if err := applyMigration(ctx, lockConn, m); err != nil {
			return fmt.Errorf("mysql: migration %06d_%s: %w", m.version, m.name, err)
		}
	}
	return nil
}

// baselineComplete reports whether every table created by the baseline
// migration exists. Only then may a legacy database without version
// bookkeeping be stamped as version 1.
func baselineComplete(ctx context.Context, q migrationQueryer) (bool, error) {
	for _, t := range requiredTables {
		exists, err := tableExists(ctx, q, t)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// applyMigration runs the migration DDL first and records the version only
// after the DDL has completed. Callers hold the migration lock (the
// per-database in-process guard +
// the MySQL named lock) and pass the lock-holding connection, so by the time
// another Migrate observes the version row, the schema it describes is
// complete. A failed migration inserts nothing, so the next Migrate retries
// the DDL instead of skipping it.
func applyMigration(ctx context.Context, q migrationQueryer, m migration) error {
	var applied int64
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationTableName+` WHERE version = ?`, m.version).Scan(&applied); err != nil {
		return err
	}
	if applied > 0 {
		return nil // already applied
	}
	for _, stmt := range splitSQL(m.up) {
		if isCreateIndex(stmt) {
			// Standalone CREATE INDEX backfills (e.g. 000003): tolerate
			// only duplicate-index errors (the index is already there);
			// every other failure aborts instead of passing silently.
			if err := execIndexBackfill(ctx, q, stmt); err != nil {
				return err
			}
			continue
		}
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			if isAddColumn(stmt) && isDuplicateColumnError(err) {
				continue // already backfilled; keep going
			}
			return fmt.Errorf("mysql migrate: %w\nstmt: %s", err, stmt)
		}
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO `+migrationTableName+` (version) VALUES (?)`, m.version); err != nil {
		if isUniqueViolation(err) {
			return nil // another migrator completed the same DDL first
		}
		return err
	}
	return nil
}

// isTiDB reports whether the server behind conn is TiDB. TiDB version
// strings look like "8.5.1-TiDB-v8.5.1"; plain MySQL/MariaDB never contain
// "TiDB". It is called once per Migrate so the named-lock path (which needs
// GET_LOCK/RELEASE_LOCK, absent on TiDB) can be skipped there.
func isTiDB(ctx context.Context, conn *sql.Conn) (bool, error) {
	var version string
	if err := conn.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		return false, fmt.Errorf("mysql: detect server flavor: %w", err)
	}
	return strings.Contains(strings.ToLower(version), "tidb"), nil
}

// acquireMigrationLock holds the per-database migration lock on conn for the
// whole migration and reports the effective lock name for release. The lock
// is scoped to the selected database (SELECT DATABASE()) so migrations for
// unrelated databases on the same server never block each other; with no
// database selected it falls back to the server-wide base name.
// GET_LOCK returns 1 on success, 0 on timeout, and NULL on error.
func acquireMigrationLock(ctx context.Context, conn *sql.Conn) (string, error) {
	var dbName sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&dbName); err != nil {
		return "", fmt.Errorf("mysql: identify migration database: %w", err)
	}
	name := migrateLockName
	if dbName.Valid && dbName.String != "" {
		name = scopedMigrationLockName(dbName.String)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 30)`, name).Scan(&got); err != nil {
		return "", fmt.Errorf("mysql: acquire migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return "", fmt.Errorf("mysql: acquire migration lock %q: timed out (another migrator holds it)", name)
	}
	return name, nil
}

// releaseMigrationLock releases a lock previously acquired by
// acquireMigrationLock. The caller passes a non-canceled context so the lock
// is released even when Migrate's context was canceled.
func releaseMigrationLock(ctx context.Context, conn *sql.Conn, name string) {
	if name == "" {
		name = migrateLockName
	}
	_, _ = conn.ExecContext(ctx, `SELECT RELEASE_LOCK(?)`, name)
}

func tableExists(ctx context.Context, q migrationQueryer, table string) (bool, error) {
	var n int64
	if err := q.QueryRowContext(ctx,
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

// isCreateIndex reports whether stmt creates an index. Standalone CREATE
// INDEX backfills run through execIndexBackfill, which tolerates only
// duplicate-index errors.
func isCreateIndex(stmt string) bool {
	upper := strings.ToUpper(strings.TrimSpace(stmt))
	return strings.HasPrefix(upper, "CREATE INDEX") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX")
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
// required table exists in the current database and that every required
// column is present; missing tables or columns are reported together in one
// error.
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
	var missingCols []string
	for _, c := range requiredColumns {
		var n int64
		if err := b.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			c[0], c[1]).Scan(&n); err != nil {
			return fmt.Errorf("mysql: validate schema: %w", err)
		}
		if n == 0 {
			missingCols = append(missingCols, c[0]+"."+c[1])
		}
	}
	if len(missingCols) > 0 {
		return fmt.Errorf("mysql: schema is missing columns %s; run Migrate (or your migration tool) first",
			strings.Join(missingCols, ", "))
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
