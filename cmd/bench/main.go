package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/bench"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres|sqlite|mysql|spanner|dynamodb|firestore")
	workers := flag.Int("workers", 4, "in-process worker count")
	instances := flag.Int("instances", 200, "workflows to start")
	steps := flag.Int("steps", 3, "serial activity steps per workflow")
	duration := flag.Duration("duration", 0, "max wait (0 = wait for all)")
	poll := flag.Duration("poll", 20*time.Millisecond, "worker poll interval")
	claimLimit := flag.Int("claim-limit", 0, "tasks per claim (0 = worker default 10)")
	activityConc := flag.Int("activity-concurrency", 0, "parallel activities (0 = worker default 1)")
	workflowConc := flag.Int("workflow-concurrency", 0, "parallel workflows (0 = worker default 1)")
	jsonOut := flag.Bool("json", false, "emit JSON result")
	flag.Parse()

	ctx := context.Background()
	b, closer, name, err := openBackend(ctx, *backendFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	res, err := bench.Run(ctx, b, name, bench.Config{
		Workers:             *workers,
		Instances:           *instances,
		Steps:               *steps,
		Duration:            *duration,
		Poll:                *poll,
		ClaimLimit:          *claimLimit,
		ActivityConcurrency: *activityConc,
		WorkflowConcurrency: *workflowConc,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		printResult(res, *jsonOut)
		os.Exit(1)
	}
	printResult(res, *jsonOut)
}

func printResult(res bench.Result, jsonOut bool) {
	if jsonOut {
		raw, jerr := res.FormatJSON()
		if jerr != nil {
			fmt.Fprintln(os.Stderr, jerr)
			os.Exit(1)
		}
		fmt.Println(string(raw))
		return
	}
	fmt.Print(res.FormatHuman())
}

func openBackend(ctx context.Context, name string) (backend.Backend, func(), string, error) {
	switch name {
	case "memory":
		return memory.New(), func() {}, "memory", nil
	case "postgres":
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			return nil, nil, "", fmt.Errorf("TASUKI_POSTGRES_DSN required")
		}
		pb, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, pb, "postgres")
	case "sqlite":
		path := os.Getenv("TASUKI_SQLITE_PATH")
		if path == "" {
			return nil, nil, "", fmt.Errorf("TASUKI_SQLITE_PATH required")
		}
		sb, err := sqlite.New(path)
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, sb, "sqlite")
	case "mysql":
		dsn := os.Getenv("TASUKI_MYSQL_DSN")
		if dsn == "" {
			return nil, nil, "", fmt.Errorf("TASUKI_MYSQL_DSN required")
		}
		mb, err := mysql.New(ctx, dsn)
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, mb, "mysql")
	case "spanner":
		dsn := os.Getenv("TASUKI_SPANNER_DSN")
		if dsn == "" {
			return nil, nil, "", fmt.Errorf("TASUKI_SPANNER_DSN required")
		}
		sp, err := spanner.New(ctx, dsn)
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, sp, "spanner")
	case "dynamodb":
		db, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: os.Getenv("TASUKI_DYNAMODB_ENDPOINT")})
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, db, "dynamodb")
	case "firestore":
		fb, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
		if err != nil {
			return nil, nil, "", err
		}
		return ready(ctx, fb, "firestore")
	default:
		return nil, nil, "", fmt.Errorf("unknown -backend=%q (want memory|postgres|sqlite|mysql|spanner|dynamodb|firestore)", name)
	}
}

type migrater interface {
	Migrate(ctx context.Context) error
	Reset(ctx context.Context) error
}

func ready(ctx context.Context, b backend.Backend, name string) (backend.Backend, func(), string, error) {
	m, ok := b.(migrater)
	if !ok {
		return nil, nil, "", fmt.Errorf("%s: missing Migrate/Reset", name)
	}
	if err := m.Migrate(ctx); err != nil {
		closeBackend(b)
		return nil, nil, "", err
	}
	if err := m.Reset(ctx); err != nil {
		closeBackend(b)
		return nil, nil, "", err
	}
	return b, func() { closeBackend(b) }, name, nil
}

func closeBackend(b backend.Backend) {
	switch c := b.(type) {
	case interface{ Close() error }:
		_ = c.Close()
	case interface{ Close() }:
		c.Close()
	}
}
