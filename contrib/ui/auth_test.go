package ui_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/client"
	"github.com/hirokazumiyaji/tasuki/contrib/ui"
)

func TestHandler_Auth(t *testing.T) {
	b := memory.New()
	c := client.NewClient(b)
	if err := b.CreateInstance(context.Background(), backend.NewInstance{
		ID: "auth-1", Name: "demo", Queue: "default", Input: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	h := ui.NewHandler(c, ui.WithToken("s3cret"))

	noAuth := httptest.NewRecorder()
	h.ServeHTTP(noAuth, httptest.NewRequest(http.MethodGet, "/", nil))
	if noAuth.Code != http.StatusUnauthorized {
		t.Fatalf("no auth want 401 got %d", noAuth.Code)
	}
	if noAuth.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate")
	}

	bad := httptest.NewRequest(http.MethodGet, "/", nil)
	bad.Header.Set("Authorization", "Bearer wrong")
	br := httptest.NewRecorder()
	h.ServeHTTP(br, bad)
	if br.Code != http.StatusUnauthorized {
		t.Fatalf("bad bearer want 401 got %d", br.Code)
	}

	ok := httptest.NewRequest(http.MethodGet, "/", nil)
	ok.Header.Set("Authorization", "Bearer s3cret")
	or := httptest.NewRecorder()
	h.ServeHTTP(or, ok)
	if or.Code != http.StatusOK {
		t.Fatalf("bearer want 200 got %d", or.Code)
	}

	basic := httptest.NewRequest(http.MethodGet, "/instances/auth-1", nil)
	basic.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("any:s3cret")))
	bsr := httptest.NewRecorder()
	h.ServeHTTP(bsr, basic)
	if bsr.Code != http.StatusOK {
		t.Fatalf("basic want 200 got %d", bsr.Code)
	}
}
