package ui

import (
	"testing"
	"time"
)

func TestVerifyCSRF_RejectsBadAndExpired(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tok := issueCSRF(secret, "inst-1", now)
	if !verifyCSRF(secret, "inst-1", tok, now) {
		t.Fatal("valid token")
	}
	if verifyCSRF(secret, "other", tok, now) {
		t.Fatal("wrong instance")
	}
	if verifyCSRF(secret, "inst-1", "%%%", now) {
		t.Fatal("bad b64")
	}
	if verifyCSRF(secret, "inst-1", "bm9waXBl", now) { // "nopipe"
		t.Fatal("missing pipe")
	}
	if verifyCSRF(secret, "inst-1", tok, now.Add(2*time.Hour)) {
		t.Fatal("expired")
	}
}
