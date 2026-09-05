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
	addr := flag.String("addr", "127.0.0.1:8080", "listen address (default loopback)")
	tokenFlag := flag.String("token", "", "shared secret (overrides TASUKI_UI_TOKEN); empty disables auth only on loopback")
	allowUnauthExternal := flag.Bool("allow-unauthenticated-external", false, "explicit opt-in to expose without auth on non-loopback (dangerous)")
	flag.Parse()

	token := *tokenFlag
	if token == "" {
		token = os.Getenv("TASUKI_UI_TOKEN")
	}

	if err := ui.ValidateAddr(*addr, token != "", *allowUnauthExternal); err != nil {
		fmt.Fprintln(os.Stderr, "tasuki-ui: ", err)
		os.Exit(2)
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
	srv := ui.NewServer(*addr, h)
	log.Printf("tasuki ui listening on http://%s backend=%s auth=%s", *addr, *backendFlag, auth)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
