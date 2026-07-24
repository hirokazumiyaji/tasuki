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
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres")
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
	default:
		return nil, nil, fmt.Errorf("unknown -backend=%q (want memory|postgres)", name)
	}
}
