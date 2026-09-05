package backendopen_test

import (
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func TestDescribeTarget_RedactsCredentials(t *testing.T) {
	t.Setenv("TASUKI_POSTGRES_DSN", "postgres://user:secret@localhost:5432/tasuki?sslmode=disable")
	got := backendopen.DescribeTarget("postgres")
	if strings.Contains(got, "secret") {
		t.Fatalf("credentials leaked: %q", got)
	}
	if !strings.Contains(got, "localhost") {
		t.Fatalf("host missing: %q", got)
	}
	if backendopen.DescribeTarget("memory") == "" {
		t.Fatal("empty memory target")
	}
}
