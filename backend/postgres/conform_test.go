package postgres_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func TestConformance(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	root, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	if err := root.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()
		if err := root.Reset(ctx); err != nil {
			t.Fatal(err)
		}
		return root
	})
}
