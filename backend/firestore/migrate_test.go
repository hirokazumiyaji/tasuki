package firestore_test

import (
	"context"
	"os"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/firestore"
)

func emulatorOrSkip(t *testing.T) {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set")
	}
}

func TestMigrateIdempotent(t *testing.T) {
	emulatorOrSkip(t)
	ctx := context.Background()
	b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
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
