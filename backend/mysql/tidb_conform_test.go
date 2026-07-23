package mysql_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func TestTiDBConformance(t *testing.T) {
	dsn := tidbDSNOrSkip(t)
	ensureTiDBDatabase(t, dsn)
	ctx := context.Background()
	root, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
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
