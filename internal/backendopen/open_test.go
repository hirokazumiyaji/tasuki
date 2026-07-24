package backendopen_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func TestOpen_Unknown(t *testing.T) {
	_, _, err := backendopen.Open(context.Background(), "nope", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("got %v", err)
	}
}

func TestOpen_SQLiteTemp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open.db")
	t.Setenv("TASUKI_SQLITE_PATH", path)
	b, closer, err := backendopen.Open(context.Background(), "sqlite", backendopen.Options{Reset: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closer()
	if b == nil {
		t.Fatal("nil backend")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_SQLiteMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SQLITE_PATH", "")
	_, _, err := backendopen.Open(context.Background(), "sqlite", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SQLITE_PATH") {
		t.Fatalf("got %v", err)
	}
}
