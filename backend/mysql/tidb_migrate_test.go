package mysql_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

func tidbDSNOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_TIDB_DSN")
	if dsn == "" {
		t.Skip("TASUKI_TIDB_DSN not set")
	}
	return dsn
}

// ensureTiDBDatabase creates the database named in dsn if missing (TiDB root has no password by default).
func ensureTiDBDatabase(t *testing.T, dsn string) {
	t.Helper()
	dbName, adminDSN, ok := splitMySQLDSN(dsn)
	if !ok || dbName == "" {
		t.Fatalf("cannot parse database from TASUKI_TIDB_DSN")
	}
	db, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`"); err != nil {
		t.Fatal(err)
	}
}

// splitMySQLDSN returns (database, dsnWithoutDatabase, ok).
func splitMySQLDSN(dsn string) (string, string, bool) {
	// user:pass@tcp(host:port)/dbname?params
	slash := strings.Index(dsn, ")/")
	if slash < 0 {
		return "", "", false
	}
	rest := dsn[slash+2:]
	q := strings.IndexByte(rest, '?')
	var dbName, params string
	if q < 0 {
		dbName = rest
	} else {
		dbName = rest[:q]
		params = rest[q:]
	}
	admin := dsn[:slash+2] + params // ...)/?params or ...)/
	return dbName, admin, true
}

func TestTiDBMigrateIdempotent(t *testing.T) {
	dsn := tidbDSNOrSkip(t)
	ensureTiDBDatabase(t, dsn)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
}
