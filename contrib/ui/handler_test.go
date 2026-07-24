package ui_test

import (
	"context"
	"net/http"
	"net/http/httptest"
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
