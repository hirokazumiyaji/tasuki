package backendopen

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// Options controls open behavior for persistent stores.
type Options struct {
	Reset bool // when true, call Reset after Migrate (bench)
}

var (
	mu       sync.RWMutex
	openers  = map[string]func(ctx context.Context) (backend.Backend, func(), error){}
)

// Register adds a backend opener (called from tasuki_all-tagged backends).
func Register(name string, fn func(ctx context.Context) (backend.Backend, func(), error)) {
	mu.Lock()
	defer mu.Unlock()
	openers[name] = fn
}

func registered(name string) (func(ctx context.Context) (backend.Backend, func(), error), bool) {
	mu.RLock()
	defer mu.RUnlock()
	fn, ok := openers[name]
	return fn, ok
}

// Open returns a backend for name (memory|postgres|sqlite|mysql|spanner|dynamodb|firestore).
// Memory is always available. Other backends require registration via the
// tasuki_all build tag (workspace build) or an explicit blank import of the
// backend's register package. The closer should be deferred by the caller.
func Open(ctx context.Context, name string, opts Options) (backend.Backend, func(), error) {
	if name == "memory" {
		return memory.New(), func() {}, nil
	}
	if fn, ok := registered(name); ok {
		b, closer, err := fn(ctx)
		if err != nil {
			return nil, nil, err
		}
		return finish(ctx, b, opts, closer)
	}
	switch name {
	case "postgres", "sqlite", "mysql", "spanner", "dynamodb", "firestore":
		// Friendly error that guides GOWORK=off users.
		return nil, nil, fmt.Errorf("backend %q not registered (build without tasuki_all tag only supports memory; build with -tags tasuki_all in the workspace or import the backend package)", name)
	default:
		return nil, nil, fmt.Errorf("unknown -backend=%q (want memory|postgres|sqlite|mysql|spanner|dynamodb|firestore)", name)
	}
}

// OpenWithDSN is a test helper that bypasses env for sqlite/memory.
func OpenWithDSN(ctx context.Context, name, dsn string, opts Options) (backend.Backend, func(), error) {
	if name == "memory" {
		return memory.New(), func() {}, nil
	}
	return Open(ctx, name, opts)
}

// DescribeTarget returns a human-readable wipe target for --reset confirmation.
func DescribeTarget(name string) string {
	switch name {
	case "memory":
		return "in-process memory (no persistent data)"
	case "postgres":
		return "TASUKI_POSTGRES_DSN=" + redactDSN(os.Getenv("TASUKI_POSTGRES_DSN"))
	case "sqlite":
		return "TASUKI_SQLITE_PATH=" + os.Getenv("TASUKI_SQLITE_PATH")
	case "mysql":
		return "TASUKI_MYSQL_DSN=" + redactDSN(os.Getenv("TASUKI_MYSQL_DSN"))
	case "spanner":
		return "TASUKI_SPANNER_DSN=" + os.Getenv("TASUKI_SPANNER_DSN")
	case "dynamodb":
		return "endpoint=" + os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	case "firestore":
		return "project=" + os.Getenv("TASUKI_FIRESTORE_PROJECT")
	default:
		return name
	}
}

func redactDSN(s string) string {
	if s == "" {
		return "(unset)"
	}
	// Keep host/db, hide credentials after :// and before @.
	if i := len("postgres://"); len(s) > i {
		rest := s[i:]
		if at := lastIndex(rest, "@"); at >= 0 {
			return s[:i] + "***@" + rest[at+1:]
		}
	}
	if at := lastIndex(s, "@"); at >= 0 {
		// mysql DSN user:pass@...
		if colon := lastIndex(s[:at], ":"); colon >= 0 {
			return s[:colon+1] + "***" + s[at:]
		}
	}
	return s
}

func lastIndex(s, sub string) int {
	for i := len(s) - len(sub); i >= 0; i-- {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
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
