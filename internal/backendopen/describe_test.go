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

func TestDescribeTarget_RedactSchemes(t *testing.T) {
	// postgresql:// scheme must be recognized as a URL, not userinfo.
	t.Setenv("TASUKI_POSTGRES_DSN", "postgresql://user:s3cret@db.internal:5432/tasuki")
	if got := backendopen.DescribeTarget("postgres"); strings.Contains(got, "s3cret") {
		t.Fatalf("credentials leaked: %q", got)
	}
	// MySQL DSNs must mask only the password, never leak its prefix.
	t.Setenv("TASUKI_MYSQL_DSN", "app_user:secretpass@tcp(127.0.0.1:3306)/tasuki")
	got := backendopen.DescribeTarget("mysql")
	if strings.Contains(got, "secretpass") {
		t.Fatalf("credentials leaked: %q", got)
	}
	if strings.Contains(got, "app_user:se") {
		t.Fatalf("password prefix leaked: %q", got)
	}
	if !strings.Contains(got, "app_user:***@") {
		t.Fatalf("unexpected masking: %q", got)
	}
	// Strings without credentials pass through untouched.
	t.Setenv("TASUKI_MYSQL_DSN", "tcp(127.0.0.1:3306)/tasuki")
	if got := backendopen.DescribeTarget("mysql"); got != "TASUKI_MYSQL_DSN=tcp(127.0.0.1:3306)/tasuki" {
		t.Fatalf("unexpected rewrite: %q", got)
	}
}
