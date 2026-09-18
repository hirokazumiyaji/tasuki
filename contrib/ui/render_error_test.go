package ui

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRender_TemplateErrorSanitized(t *testing.T) {
	s, _ := testServer(t)
	// Replace with a template missing the requested name so ExecuteTemplate fails.
	s.tmpl = template.Must(template.New("empty").Parse(`hello`))

	r := httptest.NewRequest(http.MethodGet, "/instances/x", nil)
	w := httptest.NewRecorder()
	s.render(w, r, "detail.html", detailPage{Title: "t"}, http.StatusOK)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "detail.html") || strings.Contains(body, "no template") {
		t.Fatalf("raw template error leaked: %q", body)
	}
	if !strings.Contains(body, "request id") {
		t.Fatalf("want generic message with request id, got %q", body)
	}
	if rid := w.Header().Get("X-Request-ID"); rid == "" {
		t.Fatal("missing X-Request-ID header")
	}
}
