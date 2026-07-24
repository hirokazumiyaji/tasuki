package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backend/mysql"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres|sqlite|mysql|spanner|dynamodb|firestore")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	ctx := context.Background()
	b, closer, err := openBackend(ctx, *backendFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	c := tasuki.NewClient(b)
	h := ui.NewHandler(c)
	log.Printf("tasuki ui listening on http://localhost%s backend=%s", *addr, *backendFlag)
	if err := http.ListenAndServe(*addr, h); err != nil {
		log.Fatal(err)
	}
}

func openBackend(ctx context.Context, name string) (backend.Backend, func(), error) {
	switch name {
	case "memory":
		return memory.New(), func() {}, nil
	case "postgres":
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_POSTGRES_DSN required")
		}
		pb, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		if err := pb.Migrate(ctx); err != nil {
			pb.Close()
			return nil, nil, err
		}
		return pb, pb.Close, nil
	case "sqlite":
		path := os.Getenv("TASUKI_SQLITE_PATH")
		if path == "" {
			return nil, nil, fmt.Errorf("TASUKI_SQLITE_PATH required")
		}
		sb, err := sqlite.New(path)
		if err != nil {
			return nil, nil, err
		}
		if err := sb.Migrate(ctx); err != nil {
			_ = sb.Close()
			return nil, nil, err
		}
		return sb, func() { _ = sb.Close() }, nil
	case "mysql":
		dsn := os.Getenv("TASUKI_MYSQL_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_MYSQL_DSN required")
		}
		mb, err := mysql.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		if err := mb.Migrate(ctx); err != nil {
			_ = mb.Close()
			return nil, nil, err
		}
		return mb, func() { _ = mb.Close() }, nil
	case "spanner":
		dsn := os.Getenv("TASUKI_SPANNER_DSN")
		if dsn == "" {
			return nil, nil, fmt.Errorf("TASUKI_SPANNER_DSN required")
		}
		sp, err := spanner.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		if err := sp.Migrate(ctx); err != nil {
			_ = sp.Close()
			return nil, nil, err
		}
		return sp, func() { _ = sp.Close() }, nil
	case "dynamodb":
		db, err := dynamodb.New(ctx, dynamodb.Config{
			Endpoint: os.Getenv("TASUKI_DYNAMODB_ENDPOINT"),
		})
		if err != nil {
			return nil, nil, err
		}
		if err := db.Migrate(ctx); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
		return db, func() { _ = db.Close() }, nil
	case "firestore":
		fb, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
		if err != nil {
			return nil, nil, err
		}
		if err := fb.Migrate(ctx); err != nil {
			_ = fb.Close()
			return nil, nil, err
		}
		return fb, func() { _ = fb.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown -backend=%q (want memory|postgres|sqlite|mysql|spanner|dynamodb|firestore)", name)
	}
}
