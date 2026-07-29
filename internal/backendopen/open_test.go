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

func TestOpen_Memory(t *testing.T) {
	b, closer, err := backendopen.Open(context.Background(), "memory", backendopen.Options{})
	if err != nil || b == nil {
		t.Fatalf("%v %v", b, err)
	}
	closer()
}

func TestOpen_PostgresMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_POSTGRES_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "postgres", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_POSTGRES_DSN") {
		t.Fatalf("%v", err)
	}
}

func TestOpen_MySQLMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_MYSQL_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "mysql", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_MYSQL_DSN") {
		t.Fatalf("%v", err)
	}
}

func TestOpen_SpannerMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SPANNER_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "spanner", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SPANNER_DSN") {
		t.Fatalf("%v", err)
	}
}
