//go:build tasuki_all

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func TestOpenBackend_Unknown(t *testing.T) {
	_, _, err := backendopen.Open(context.Background(), "nope", backendopen.Options{Reset: true})
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("got %v", err)
	}
}

func TestOpenBackend_SQLiteTemp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.db")
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
