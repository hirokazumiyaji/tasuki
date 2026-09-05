//go:build tasuki_all

package backendopen

import (
	"context"
	"fmt"
	"os"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

func init() {
	Register("postgres", func(ctx context.Context) (backend.Backend, func(), error) {
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_POSTGRES_DSN required")
		}
		b, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return b, func() { b.Close() }, nil
	})
	Register("sqlite", func(ctx context.Context) (backend.Backend, func(), error) {
		path := os.Getenv("TASUKI_SQLITE_PATH")
		if path == "" {
			return nil, nil, fmt.Errorf("TASUKI_SQLITE_PATH required")
		}
		b, err := sqlite.New(path)
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	})
	Register("mysql", func(ctx context.Context) (backend.Backend, func(), error) {
		dsn := os.Getenv("TASUKI_MYSQL_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_MYSQL_DSN required")
		}
		b, err := mysql.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	})
	Register("spanner", func(ctx context.Context) (backend.Backend, func(), error) {
		dsn := os.Getenv("TASUKI_SPANNER_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_SPANNER_DSN required")
		}
		b, err := spanner.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	})
	Register("dynamodb", func(ctx context.Context) (backend.Backend, func(), error) {
		b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: os.Getenv("TASUKI_DYNAMODB_ENDPOINT")})
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	})
	Register("firestore", func(ctx context.Context) (backend.Backend, func(), error) {
		b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	})
}
