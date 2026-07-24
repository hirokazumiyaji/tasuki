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
	tokenFlag := flag.String("token", "", "shared secret (overrides TASUKI_UI_TOKEN); empty disables auth")
	flag.Parse()

	token := *tokenFlag
	if token == "" {
		token = os.Getenv("TASUKI_UI_TOKEN")
	}

	ctx := context.Background()
	b, closer, err := backendopen.Open(ctx, *backendFlag, backendopen.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	c := tasuki.NewClient(b)
	var opts []ui.Option
	auth := "off"
	if token != "" {
		opts = append(opts, ui.WithToken(token))
		auth = "on"
	}
	h := ui.NewHandler(c, opts...)
	log.Printf("tasuki ui listening on http://localhost%s backend=%s auth=%s", *addr, *backendFlag, auth)
	if err := http.ListenAndServe(*addr, h); err != nil {
		log.Fatal(err)
	}
}
