package ui_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

// failListBackend forces ListInstances to fail with a sensitive message.
type failListBackend struct {
	backend.Backend
	err error
}

func (f *failListBackend) ListInstances(_ context.Context, _ backend.InstanceFilter) ([]backend.Instance, error) {
	return nil, f.err
}

// failGetBackend forces GetInstance to fail with a non-NotFound error.
type failGetBackend struct {
	backend.Backend
	err error
}

func (f *failGetBackend) GetInstance(_ context.Context, _ string) (*backend.Instance, error) {
	return nil, f.err
}

func TestHandler_ListErrorSanitized(t *testing.T) {
	sensitive := errors.New("postgres dial SECRET-TOKEN-123")
	b := &failListBackend{Backend: memory.New(), err: sensitive}
	h := ui.NewHandler(tasuki.NewClient(b))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "SECRET-TOKEN-123") {
		t.Fatalf("raw backend error leaked to client: %s", body)
	}
	if !strings.Contains(body, "request id") {
		t.Fatalf("generic message should carry request id: %s", body)
	}
	if rid := w.Header().Get("X-Request-ID"); rid == "" {
		t.Fatal("missing X-Request-ID header")
	}
}

func TestHandler_DetailErrorSanitized(t *testing.T) {
	sensitive := errors.New("journal read SECRET-TOKEN-456")
	b := &failGetBackend{Backend: memory.New(), err: sensitive}
	h := ui.NewHandler(tasuki.NewClient(b))

	r := httptest.NewRequest(http.MethodGet, "/instances/anything", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "SECRET-TOKEN-456") {
		t.Fatalf("raw backend error leaked to client: %s", body)
	}
	if !strings.Contains(body, "request id") {
		t.Fatalf("generic message should carry request id: %s", body)
	}
	if rid := w.Header().Get("X-Request-ID"); rid == "" {
		t.Fatal("missing X-Request-ID header")
	}
}

func TestHandler_DetailNotFoundMessage(t *testing.T) {
	b := memory.New()
	h := ui.NewHandler(tasuki.NewClient(b))

	r := httptest.NewRequest(http.MethodGet, "/instances/nope", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "instance not found") {
		t.Fatalf("want sanitized not-found message, got: %s", w.Body.String())
	}
	if rid := w.Header().Get("X-Request-ID"); rid == "" {
		t.Fatal("missing X-Request-ID header")
	}
}

func TestHandler_ListRequestIDPassthrough(t *testing.T) {
	b := memory.New()
	h := ui.NewHandler(tasuki.NewClient(b))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Request-ID", "test-req-1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get("X-Request-ID"); got != "test-req-1" {
		t.Fatalf("X-Request-ID echo=%q want test-req-1", got)
	}
}

func TestHandler_MiddlewareRequestIDPropagated(t *testing.T) {
	sensitive := errors.New("detail read SECRET-TOKEN-789")
	b := &failGetBackend{Backend: memory.New(), err: sensitive}
	inner := ui.NewHandler(tasuki.NewClient(b))
	// Outer middleware sets the ID on the response before delegating.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", "mw-id-1")
		inner.ServeHTTP(w, r)
	})

	r := httptest.NewRequest(http.MethodGet, "/instances/anything", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get("X-Request-ID"); got != "mw-id-1" {
		t.Fatalf("X-Request-ID echo=%q want mw-id-1", got)
	}
	body := w.Body.String()
	if !strings.Contains(body, "mw-id-1") {
		t.Fatalf("middleware request ID not propagated to error page: %s", body)
	}
	if strings.Contains(body, "SECRET-TOKEN-789") {
		t.Fatalf("raw backend error leaked to client: %s", body)
	}
}
