package ui_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func TestHandler_ListAndDetail(t *testing.T) {
	b := memory.New()
	c := tasuki.NewClient(b)
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "ui-1", Name: "demo", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}

	h := ui.NewHandler(c)

	list := httptest.NewRequest(http.MethodGet, "/", nil)
	lr := httptest.NewRecorder()
	h.ServeHTTP(lr, list)
	if lr.Code != http.StatusOK {
		t.Fatalf("list status=%d", lr.Code)
	}
	body := lr.Body.String()
	if !strings.Contains(body, "ui-1") || !strings.Contains(body, "tasuki") {
		t.Fatalf("list body missing instance: %s", body)
	}

	detail := httptest.NewRequest(http.MethodGet, "/instances/ui-1", nil)
	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, detail)
	if dr.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", dr.Code, dr.Body.String())
	}
	if !strings.Contains(dr.Body.String(), "workflow_started") && !strings.Contains(dr.Body.String(), "demo") {
		t.Fatalf("detail missing content: %s", dr.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet, "/instances/nope", nil)
	mr := httptest.NewRecorder()
	h.ServeHTTP(mr, missing)
	if mr.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d", mr.Code)
	}
}

func TestHandler_Terminate(t *testing.T) {
	b := memory.New()
	c := tasuki.NewClient(b)
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "term-ui", Name: "demo", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	h := ui.NewHandler(c)

	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, httptest.NewRequest(http.MethodGet, "/instances/term-ui", nil))
	if dr.Code != http.StatusOK {
		t.Fatalf("detail %d", dr.Code)
	}
	body := dr.Body.String()
	if !strings.Contains(body, `name="csrf"`) || !strings.Contains(body, "Terminate") {
		t.Fatalf("missing form: %s", body)
	}
	csrf := extractInputValue(t, body, "csrf")

	bad := httptest.NewRequest(http.MethodPost, "/instances/term-ui/terminate", strings.NewReader("csrf=bad&confirm=1"))
	bad.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	br := httptest.NewRecorder()
	h.ServeHTTP(br, bad)
	if br.Code != http.StatusForbidden {
		t.Fatalf("bad csrf want 403 got %d", br.Code)
	}

	nc := httptest.NewRequest(http.MethodPost, "/instances/term-ui/terminate", strings.NewReader("csrf="+url.QueryEscape(csrf)))
	nc.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	nr := httptest.NewRecorder()
	h.ServeHTTP(nr, nc)
	if nr.Code != http.StatusBadRequest {
		t.Fatalf("no confirm want 400 got %d", nr.Code)
	}

	inst, err := c.Get(ctx, "term-ui")
	if err != nil || inst.Status != tasuki.StatusRunning {
		t.Fatalf("still running want; status=%v err=%v", inst, err)
	}

	ok := httptest.NewRequest(http.MethodPost, "/instances/term-ui/terminate", strings.NewReader("csrf="+url.QueryEscape(csrf)+"&confirm=1"))
	ok.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	or := httptest.NewRecorder()
	h.ServeHTTP(or, ok)
	if or.Code != http.StatusSeeOther {
		t.Fatalf("want 303 got %d body=%s", or.Code, or.Body.String())
	}
	if loc := or.Header().Get("Location"); loc != "/instances/term-ui" {
		t.Fatalf("Location=%q", loc)
	}
	inst, err = c.Get(ctx, "term-ui")
	if err != nil || inst.Status != tasuki.StatusTerminated {
		t.Fatalf("status=%v err=%v", inst, err)
	}

	dr2 := httptest.NewRecorder()
	h.ServeHTTP(dr2, httptest.NewRequest(http.MethodGet, "/instances/term-ui", nil))
	if strings.Contains(dr2.Body.String(), "Terminate") {
		t.Fatal("form should be hidden after terminate")
	}
}

var inputValueRe = regexp.MustCompile(`name="([^"]+)" value="([^"]*)"`)

func extractInputValue(t *testing.T, html, name string) string {
	t.Helper()
	for _, m := range inputValueRe.FindAllStringSubmatch(html, -1) {
		if m[1] == name {
			return m[2]
		}
	}
	t.Fatalf("input %q not found in html", name)
	return ""
}
