package mysql_test

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TASUKI_MYSQL_DSN not set")
	}
	return dsn
}

func TestMigrateIdempotent(t *testing.T) {
	dsn := dsnOrSkip(t)
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
