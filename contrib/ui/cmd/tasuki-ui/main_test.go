//go:build tasuki_all

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func TestOpenBackend_Unknown(t *testing.T) {
	_, _, err := backendopen.Open(context.Background(), "nope", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("want unknown backend error, got %v", err)
	}
}

func TestOpenBackend_SQLiteMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SQLITE_PATH", "")
	_, _, err := backendopen.Open(context.Background(), "sqlite", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SQLITE_PATH") {
		t.Fatalf("want path required, got %v", err)
	}
}

func TestOpenBackend_MySQLMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_MYSQL_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "mysql", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_MYSQL_DSN") {
		t.Fatalf("want dsn required, got %v", err)
	}
}

func TestOpenBackend_SpannerMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SPANNER_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "spanner", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SPANNER_DSN") {
		t.Fatalf("want dsn required, got %v", err)
	}
}
