package dynamodb_test

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
)

func endpointOrSkip(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("TASUKI_DYNAMODB_ENDPOINT not set")
	}
	return ep
}

func TestMigrateIdempotent(t *testing.T) {
	ep := endpointOrSkip(t)
	ctx := context.Background()
	b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: ep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}
}
