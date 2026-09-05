package ui

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type options struct {
	token string
}

// Option configures NewHandler.
type Option func(*options)

// WithToken requires Bearer or HTTP Basic (password = token) when non-empty.
func WithToken(token string) Option {
	return func(o *options) { o.token = token }
}

func withAuth(next http.Handler, token string) http.Handler {
	if token == "" {
		return next
	}
	want := tokenDigest(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := extractToken(r)
		if !ok || !tokenEqual(want, tokenDigest(got)) {
			w.Header().Set("WWW-Authenticate", `Basic realm="tasuki"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", false
	}
	const bearer = "Bearer "
	if len(h) > len(bearer) && strings.EqualFold(h[:len(bearer)], bearer) {
		t := strings.TrimSpace(h[len(bearer):])
		return t, t != ""
	}
	user, pass, ok := r.BasicAuth()
	_ = user
	return pass, ok && pass != ""
}

func tokenDigest(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

func tokenEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// NewServer returns an http.Server with safe timeouts for the admin UI.
// ReadHeaderTimeout mitigates Slowloris; IdleTimeout bounds idle keep-alives.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// ValidateAddr enforces safe defaults: loopback binds may run without auth
// (single-user dev), but non-loopback binds require a token unless the
// explicit --allow-unauthenticated-external opt-in is set.
func ValidateAddr(addr string, hasToken, allowUnauthExternal bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid --addr %q: %w", addr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if !hasToken && !allowUnauthExternal {
			return fmt.Errorf("refusing unauthenticated external bind %q: set --token/TASUKI_UI_TOKEN or --allow-unauthenticated-external", addr)
		}
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && !ip.IsLoopback() {
		if !hasToken && !allowUnauthExternal {
			return fmt.Errorf("refusing unauthenticated external bind %q: set --token/TASUKI_UI_TOKEN or --allow-unauthenticated-external", addr)
		}
		return nil
	}
	if ip == nil {
		// Hostname: resolve conservatively. Non-localhost names require auth
		// unless explicitly opted in.
		lower := strings.ToLower(host)
		if lower != "localhost" && lower != "127.0.0.1" && lower != "::1" {
			if !hasToken && !allowUnauthExternal {
				return fmt.Errorf("refusing unauthenticated bind to %q: set --token/TASUKI_UI_TOKEN or --allow-unauthenticated-external", addr)
			}
		}
	}
	return nil
}
