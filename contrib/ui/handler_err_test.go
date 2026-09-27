package ui

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
)

func testServer(t *testing.T) (*server, http.Handler) {
	t.Helper()
	b := memory.New()
	c := client.NewClient(b)
	tmpl := template.Must(template.New("").ParseFS(templateFS, "templates/*.html"))
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i + 1)
	}
	s := &server{client: c, tmpl: tmpl, secret: secret}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleList)
	mux.HandleFunc("GET /instances/{id}", s.handleDetail)
	mux.HandleFunc("POST /instances/{id}/terminate", s.handleTerminate)
	mux.HandleFunc("POST /instances/{id}/signal", s.handleSignal)
	mux.HandleFunc("POST /instances/{id}/cancel", s.handleCancel)
	return s, mux
}

func TestHandleTerminate_NotFound(t *testing.T) {
	s, h := testServer(t)
	csrf := issueCSRF(s.secret, "nope", time.Now())
	body := "csrf=" + url.QueryEscape(csrf) + "&confirm=1"
	r := httptest.NewRequest(http.MethodPost, "/instances/nope/terminate", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHandleCancel_NotFound(t *testing.T) {
	s, h := testServer(t)
	csrf := issueCSRF(s.secret, "nope", time.Now())
	body := "csrf=" + url.QueryEscape(csrf) + "&confirm=1"
	r := httptest.NewRequest(http.MethodPost, "/instances/nope/cancel", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d", w.Code)
	}
}

func TestHandleSignal_NotFound(t *testing.T) {
	s, h := testServer(t)
	csrf := issueCSRF(s.secret, "nope", time.Now())
	body := "csrf=" + url.QueryEscape(csrf) + "&name=ping&payload={}"
	r := httptest.NewRequest(http.MethodPost, "/instances/nope/signal", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d", w.Code)
	}
}
