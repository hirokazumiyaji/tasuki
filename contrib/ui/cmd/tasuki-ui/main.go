package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres|sqlite|mysql|spanner|dynamodb|firestore")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	ctx := context.Background()
	b, closer, err := backendopen.Open(ctx, *backendFlag, backendopen.Options{})
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
