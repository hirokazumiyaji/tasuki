package ui_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func seedInstances(t *testing.T, b *memory.Backend, n int, prefix string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%03d", prefix, i)
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "demo", Queue: "default", Input: []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func getList(t *testing.T, h http.Handler, target string) (int, string, http.Header) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code, w.Body.String(), w.Result().Header
}

func countRows(body string) int {
	return strings.Count(body, `class="row"`)
}

func TestHandler_ListPagination(t *testing.T) {
	b := memory.New()
	seedInstances(t, b, 201, "page")
	h := ui.NewHandler(tasuki.NewClient(b))

	code, body, _ := getList(t, h, "/?limit=100")
	if code != http.StatusOK {
		t.Fatalf("page1 status=%d", code)
	}
	if got := countRows(body); got != 100 {
		t.Fatalf("page1 rows=%d want 100", got)
	}
	if !strings.Contains(body, "offset=100") || !strings.Contains(body, "Next") {
		t.Fatalf("page1 missing Next link: %s", body[len(body)-500:])
	}
	if !strings.Contains(body, "さらにあります") {
		t.Fatal("page1 missing truncation notice")
	}

	code, body, _ = getList(t, h, "/?limit=100&offset=100")
	if code != http.StatusOK {
		t.Fatalf("page2 status=%d", code)
	}
	if got := countRows(body); got != 100 {
		t.Fatalf("page2 rows=%d want 100", got)
	}
	if !strings.Contains(body, "offset=200") {
		t.Fatal("page2 missing Next to offset=200")
	}
	if !strings.Contains(body, "Prev") {
		t.Fatal("page2 missing Prev link")
	}

	code, body, _ = getList(t, h, "/?limit=100&offset=200")
	if code != http.StatusOK {
		t.Fatalf("page3 status=%d", code)
	}
	if got := countRows(body); got != 1 {
		t.Fatalf("page3 rows=%d want 1", got)
	}
	if strings.Contains(body, "さらにあります") {
		t.Fatal("page3 should not show truncation notice")
	}
	if !strings.Contains(body, "Prev") {
		t.Fatal("page3 missing Prev link")
	}
}

func TestHandler_ListPaginationBounds(t *testing.T) {
	b := memory.New()
	seedInstances(t, b, 201, "bound")
	h := ui.NewHandler(tasuki.NewClient(b))

	// Invalid limit falls back to default (100).
	_, body, _ := getList(t, h, "/?limit=abc")
	if got := countRows(body); got != 100 {
		t.Fatalf("invalid limit rows=%d want 100", got)
	}
	// Zero/negative limit falls back to default.
	_, body, _ = getList(t, h, "/?limit=0")
	if got := countRows(body); got != 100 {
		t.Fatalf("zero limit rows=%d want 100", got)
	}
	// Negative offset treated as 0.
	_, body, _ = getList(t, h, "/?limit=10&offset=-5")
	if got := countRows(body); got != 10 {
		t.Fatalf("negative offset rows=%d want 10", got)
	}
	// Huge limit is clamped: all 201 fit, so no truncation notice.
	_, body, _ = getList(t, h, "/?limit=100000")
	if got := countRows(body); got != 201 {
		t.Fatalf("clamped limit rows=%d want 201", got)
	}
	if strings.Contains(body, "さらにあります") {
		t.Fatal("clamped full list should not show truncation notice")
	}
	// Offset past the end shows empty state, not an error.
	code, body, _ := getList(t, h, "/?limit=100&offset=9999")
	if code != http.StatusOK {
		t.Fatalf("past-end status=%d", code)
	}
	if !strings.Contains(body, "No instances.") {
		t.Fatalf("past-end missing empty state: %.200s", body)
	}
	// Filters are preserved in the Next link.
	_, body, _ = getList(t, h, "/?status=running&limit=2")
	if !strings.Contains(body, "status=running") {
		t.Fatal("Next link should preserve status filter")
	}
}
