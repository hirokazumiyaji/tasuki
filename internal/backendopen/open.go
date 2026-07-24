package backendopen

import (
	"context"
	"fmt"
	"os"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// Options controls open behavior for persistent stores.
type Options struct {
	Reset bool // when true, call Reset after Migrate (bench)
}

// Open returns a backend for name (memory|postgres|sqlite|mysql|spanner|dynamodb|firestore).
// The closer should be deferred by the caller.
func Open(ctx context.Context, name string, opts Options) (backend.Backend, func(), error) {
	switch name {
	case "memory":
		return memory.New(), func() {}, nil
	case "postgres":
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_POSTGRES_DSN required")
		}
		b, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { b.Close() })
	case "sqlite":
		path := os.Getenv("TASUKI_SQLITE_PATH")
		if path == "" {
			return nil, nil, fmt.Errorf("TASUKI_SQLITE_PATH required")
		}
		b, err := sqlite.New(path)
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { _ = b.Close() })
	case "mysql":
		dsn := os.Getenv("TASUKI_MYSQL_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_MYSQL_DSN required")
		}
		b, err := mysql.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { _ = b.Close() })
	case "spanner":
		dsn := os.Getenv("TASUKI_SPANNER_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_SPANNER_DSN required")
		}
		b, err := spanner.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { _ = b.Close() })
	case "dynamodb":
		b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: os.Getenv("TASUKI_DYNAMODB_ENDPOINT")})
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { _ = b.Close() })
	case "firestore":
		b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, func() { _ = b.Close() })
	default:
		return nil, nil, fmt.Errorf("unknown -backend=%q (want memory|postgres|sqlite|mysql|spanner|dynamodb|firestore)", name)
	}
}

type preparer interface {
	Migrate(ctx context.Context) error
	Reset(ctx context.Context) error
}

func finish(ctx context.Context, b backend.Backend, opts Options, closer func()) (backend.Backend, func(), error) {
	p, ok := b.(preparer)
	if !ok {
		return nil, nil, fmt.Errorf("backend missing Migrate/Reset")
	}
	if err := p.Migrate(ctx); err != nil {
		closer()
		return nil, nil, err
	}
	if opts.Reset {
		if err := p.Reset(ctx); err != nil {
			closer()
			return nil, nil, err
		}
	}
	return b, closer, nil
}
