package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenBackend_Unknown(t *testing.T) {
	_, _, _, err := openBackend(context.Background(), "nope")
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("got %v", err)
	}
}

func TestOpenBackend_SQLiteTemp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.db")
	t.Setenv("TASUKI_SQLITE_PATH", path)
	b, closer, name, err := openBackend(context.Background(), "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer closer()
	if b == nil || name != "sqlite" {
		t.Fatalf("b=%v name=%s", b, name)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
