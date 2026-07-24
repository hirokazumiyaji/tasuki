package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenBackend_Unknown(t *testing.T) {
	_, _, err := openBackend(context.Background(), "nope")
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("want unknown backend error, got %v", err)
	}
}

func TestOpenBackend_SQLiteTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasuki.db")
	t.Setenv("TASUKI_SQLITE_PATH", path)
	b, closer, err := openBackend(context.Background(), "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer closer()
	if b == nil {
		t.Fatal("nil backend")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("db file: %v", err)
	}
}

func TestOpenBackend_SQLiteMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SQLITE_PATH", "")
	_, _, err := openBackend(context.Background(), "sqlite")
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SQLITE_PATH") {
		t.Fatalf("want path required, got %v", err)
	}
}

func TestOpenBackend_MySQLMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_MYSQL_DSN", "")
	_, _, err := openBackend(context.Background(), "mysql")
	if err == nil || !strings.Contains(err.Error(), "TASUKI_MYSQL_DSN") {
		t.Fatalf("want dsn required, got %v", err)
	}
}

func TestOpenBackend_SpannerMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SPANNER_DSN", "")
	_, _, err := openBackend(context.Background(), "spanner")
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SPANNER_DSN") {
		t.Fatalf("want dsn required, got %v", err)
	}
}
