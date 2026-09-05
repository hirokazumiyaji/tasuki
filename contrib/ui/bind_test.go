package ui_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func TestValidateAddr(t *testing.T) {
	// Default loopback without auth is allowed.
	if err := ui.ValidateAddr("127.0.0.1:8080", false, false); err != nil {
		t.Fatalf("loopback without auth should pass: %v", err)
	}
	if err := ui.ValidateAddr("localhost:8080", false, false); err != nil {
		t.Fatalf("localhost without auth should pass: %v", err)
	}
	// External without auth is refused.
	if err := ui.ValidateAddr(":8080", false, false); err == nil {
		t.Fatal("empty host without auth should fail")
	}
	if err := ui.ValidateAddr("0.0.0.0:8080", false, false); err == nil {
		t.Fatal("0.0.0.0 without auth should fail")
	}
	if err := ui.ValidateAddr("192.168.1.10:8080", false, false); err == nil {
		t.Fatal("LAN IP without auth should fail")
	}
	// External with auth passes.
	if err := ui.ValidateAddr("0.0.0.0:8080", true, false); err != nil {
		t.Fatalf("external with token should pass: %v", err)
	}
	if err := ui.ValidateAddr(":8080", true, false); err != nil {
		t.Fatalf("empty host with token should pass: %v", err)
	}
	// Explicit opt-in passes without auth (dangerous, but intentional).
	if err := ui.ValidateAddr("0.0.0.0:8080", false, true); err != nil {
		t.Fatalf("opt-in should pass: %v", err)
	}
}

func TestNewServerTimeouts(t *testing.T) {
	srv := ui.NewServer("127.0.0.1:8080", nil)
	if srv.ReadHeaderTimeout <= 0 {
		t.Fatal("ReadHeaderTimeout must be set")
	}
	if srv.IdleTimeout <= 0 {
		t.Fatal("IdleTimeout must be set")
	}
}
