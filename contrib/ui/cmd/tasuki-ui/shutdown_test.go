package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func waitForServe(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server %s did not start", addr)
}

func TestRunWithShutdown_DrainsInFlight(t *testing.T) {
	entered := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Handler: handler}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWithShutdown(ctx, srv, 5*time.Second, func() error { return srv.Serve(l) })
	}()
	waitForServe(t, addr)

	// Start a request that will be in flight when shutdown begins.
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	select {
	case <-entered: // handler is in flight; safe to initiate shutdown
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}
	cancel() // simulate SIGTERM

	select {
	case err := <-errCh:
		t.Fatalf("in-flight request failed: %v", err)
	case resp := <-respCh:
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != "ok" {
			t.Fatalf("in-flight status=%d body=%q", resp.StatusCode, body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete after shutdown")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithShutdown=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRunWithShutdown_ServeError(t *testing.T) {
	srv := &http.Server{}
	want := errors.New("boom")
	err := runWithShutdown(context.Background(), srv, time.Second, func() error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("got %v want %v", err, want)
	}
}
