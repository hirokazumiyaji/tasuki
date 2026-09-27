package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

// shutdownTimeout bounds graceful shutdown after SIGINT/SIGTERM so the
// process does not hang forever on stuck connections.
const shutdownTimeout = 10 * time.Second

// runWithShutdown serves via serve until it fails or ctx is cancelled
// (e.g. SIGTERM). On cancellation it gracefully shuts down srv, waiting for
// in-flight requests up to timeout. serve is srv.ListenAndServe in
// production; tests inject srv.Serve with a test listener.
func runWithShutdown(ctx context.Context, srv *http.Server, timeout time.Duration, serve func() error) error {
	errCh := make(chan error, 1)
	go func() { errCh <- serve() }()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	err := <-errCh
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	b, closer, err := backendopen.Open(ctx, *backendFlag, backendopen.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	c := client.NewClient(b)
	var opts []ui.Option
	auth := "off"
	if token != "" {
		opts = append(opts, ui.WithToken(token))
		auth = "on"
	}
	h := ui.NewHandler(c, opts...)
	srv := ui.NewServer(*addr, h)
	log.Printf("tasuki ui listening on http://%s backend=%s auth=%s", *addr, *backendFlag, auth)
	if err := runWithShutdown(ctx, srv, shutdownTimeout, srv.ListenAndServe); err != nil {
		log.Fatal(err)
	}
}
