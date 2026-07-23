package spanner_test

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/spanner"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TASUKI_SPANNER_DSN")
	if dsn == "" {
		t.Skip("TASUKI_SPANNER_DSN not set")
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set")
	}
	return dsn
}

func TestMigrateIdempotent(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	if err := spanner.EnsureDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := spanner.New(ctx, dsn)
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
