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
	"github.com/hirokazumiyaji/tasuki/journal"
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

func TestHandler_Signal(t *testing.T) {
	b := memory.New()
	c := tasuki.NewClient(b)
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sig-ui", Name: "demo", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	h := ui.NewHandler(c)

	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, httptest.NewRequest(http.MethodGet, "/instances/sig-ui", nil))
	body := dr.Body.String()
	if !strings.Contains(body, `action="/instances/sig-ui/signal"`) {
		t.Fatalf("missing signal form: %s", body)
	}
	csrf := extractInputValue(t, body, "csrf")

	bad := formPost("/instances/sig-ui/signal", "csrf=bad&name=ping&payload={}")
	br := httptest.NewRecorder()
	h.ServeHTTP(br, bad)
	if br.Code != http.StatusForbidden {
		t.Fatalf("want 403 got %d", br.Code)
	}

	emptyName := formPost("/instances/sig-ui/signal", "csrf="+url.QueryEscape(csrf)+"&name=&payload={}")
	er := httptest.NewRecorder()
	h.ServeHTTP(er, emptyName)
	if er.Code != http.StatusBadRequest {
		t.Fatalf("empty name want 400 got %d", er.Code)
	}

	badJSON := formPost("/instances/sig-ui/signal", "csrf="+url.QueryEscape(csrf)+"&name=ping&payload={")
	jr := httptest.NewRecorder()
	h.ServeHTTP(jr, badJSON)
	if jr.Code != http.StatusBadRequest {
		t.Fatalf("bad json want 400 got %d", jr.Code)
	}

	ok := formPost("/instances/sig-ui/signal", "csrf="+url.QueryEscape(csrf)+"&name=ping&payload="+url.QueryEscape(`{"ok":true}`))
	or := httptest.NewRecorder()
	h.ServeHTTP(or, ok)
	if or.Code != http.StatusSeeOther {
		t.Fatalf("want 303 got %d body=%s", or.Code, or.Body.String())
	}
	head, err := b.LoadWorkflowHead(ctx, "sig-ui")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, in := range head.Inbox {
		if in.Event.Type == journal.TypeSignalReceived && in.Event.Name == "ping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("inbox missing signal: %+v", head.Inbox)
	}
}

func TestHandler_Cancel(t *testing.T) {
	b := memory.New()
	c := tasuki.NewClient(b)
	ctx := context.Background()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "cancel-ui", Name: "demo", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	h := ui.NewHandler(c)

	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, httptest.NewRequest(http.MethodGet, "/instances/cancel-ui", nil))
	body := dr.Body.String()
	if !strings.Contains(body, `action="/instances/cancel-ui/cancel"`) {
		t.Fatalf("missing cancel form: %s", body)
	}
	csrf := extractInputValue(t, body, "csrf")

	bad := formPost("/instances/cancel-ui/cancel", "csrf=bad&confirm=1")
	br := httptest.NewRecorder()
	h.ServeHTTP(br, bad)
	if br.Code != http.StatusForbidden {
		t.Fatalf("want 403 got %d", br.Code)
	}

	nc := formPost("/instances/cancel-ui/cancel", "csrf="+url.QueryEscape(csrf))
	nr := httptest.NewRecorder()
	h.ServeHTTP(nr, nc)
	if nr.Code != http.StatusBadRequest {
		t.Fatalf("want 400 got %d", nr.Code)
	}

	ok := formPost("/instances/cancel-ui/cancel", "csrf="+url.QueryEscape(csrf)+"&confirm=1")
	or := httptest.NewRecorder()
	h.ServeHTTP(or, ok)
	if or.Code != http.StatusSeeOther {
		t.Fatalf("want 303 got %d body=%s", or.Code, or.Body.String())
	}
	head, err := b.LoadWorkflowHead(ctx, "cancel-ui")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, in := range head.Inbox {
		if in.Event.Type == journal.TypeCancelRequested {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("inbox missing cancel: %+v", head.Inbox)
	}
	inst, err := c.Get(ctx, "cancel-ui")
	if err != nil || inst.Status != tasuki.StatusRunning {
		t.Fatalf("still running want; status=%v err=%v", inst, err)
	}
}

func formPost(path, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
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
