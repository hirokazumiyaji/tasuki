package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "tasuki.db")
		b, err := sqlite.New(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		return b
	})
}

func TestFairDispatch(t *testing.T) {
	backendtest.RunFairDispatch(t, func(t *testing.T) backend.Backend {
		t.Helper()
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "tasuki.db")
		b, err := sqlite.New(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		if err := b.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		return b
	})
}
