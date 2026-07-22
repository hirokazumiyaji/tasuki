package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/postgres"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
	return dsn
}

func TestMigrate(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
}
