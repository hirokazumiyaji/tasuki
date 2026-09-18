package mysql_test

import (
	"context"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend/mysql"
)

// TestMigrateCompletedAtIndex verifies the PurgeInstances backing index
// exists after Migrate, and that re-running Migrate on an already-migrated
// database is a no-op (MySQL has no CREATE INDEX IF NOT EXISTS, so the
// duplicate-key error must be tolerated; issue #294, cf. #292).
func TestMigrateCompletedAtIndex(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := mysql.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	var n int
	if err := b.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'wf_instances' AND index_name = 'wf_instances_completed_at_idx'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("wf_instances_completed_at_idx missing after Migrate")
	}
}
