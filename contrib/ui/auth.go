package ui

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
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
