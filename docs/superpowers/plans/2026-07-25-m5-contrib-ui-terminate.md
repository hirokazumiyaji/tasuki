# M5 Contrib UI Terminate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Add confirm + HMAC-CSRF Terminate on the contrib UI detail page.

**Architecture:** `NewHandler` holds a process-local HMAC secret; detail GET issues a short-lived token; `POST /instances/{id}/terminate` verifies CSRF + confirm checkbox, calls `Client.Terminate`, redirects 303.

**Tech Stack:** net/http, crypto/hmac, embed templates, existing `tasuki.Client`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-contrib-ui-terminate-design.md](../specs/2026-07-25-m5-contrib-ui-terminate-design.md)

## Global Constraints

- Terminate only (no Signal); contrib UI only; no engine/Backend API changes
- Form only when `status == running`
- `[skip ci]` on commits/merges; one PR per task

## File map

| File | Responsibility |
|---|---|
| `contrib/ui/csrf.go` | Issue/verify HMAC tokens |
| `contrib/ui/handler.go` | Wire POST route + form fields on detail |
| `contrib/ui/templates/detail.html` | Terminate form |
| `contrib/ui/handler_test.go` | httptest CSRF / confirm / success |
| `README.md` | Note terminate capability |

---

### Task 1: Spec + Plan docs

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-contrib-ui-terminate-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-contrib-ui-terminate.md`
- Modify: `.claude/tasks/todo.md` (track tasks)

- [ ] **Step 1:** Ensure spec + this plan are on a branch; commit with `[skip ci]`
- [ ] **Step 2:** PR + merge to `main`

---

### Task 2: CSRF helpers + Terminate route + tests + README

**Files:**
- Create: `contrib/ui/csrf.go`
- Modify: `contrib/ui/handler.go`
- Modify: `contrib/ui/templates/detail.html`
- Modify: `contrib/ui/handler_test.go`
- Modify: `README.md`

**Interfaces:**
- Produces: `issueCSRF(secret []byte, instanceID string, exp time.Time) string`
- Produces: `verifyCSRF(secret []byte, instanceID, token string, now time.Time) bool`
- Token format: `base64.RawURLEncoding(expUnix|mac)` where `mac = HMAC-SHA256(secret, instanceID + "\n" + expUnix)`

- [ ] **Step 1: Failing tests**

Extend `contrib/ui/handler_test.go`:

```go
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

	// GET detail → form + csrf
	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, httptest.NewRequest(http.MethodGet, "/instances/term-ui", nil))
	if dr.Code != 200 {
		t.Fatalf("detail %d", dr.Code)
	}
	body := dr.Body.String()
	if !strings.Contains(body, `name="csrf"`) || !strings.Contains(body, "Terminate") {
		t.Fatalf("missing form: %s", body)
	}
	csrf := extractInputValue(body, "csrf") // helper: parse name="csrf" value="..."

	// bad csrf → 403
	bad := httptest.NewRequest(http.MethodPost, "/instances/term-ui/terminate", strings.NewReader("csrf=bad&confirm=1"))
	bad.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	br := httptest.NewRecorder()
	h.ServeHTTP(br, bad)
	if br.Code != http.StatusForbidden {
		t.Fatalf("bad csrf want 403 got %d", br.Code)
	}

	// no confirm → 400
	nc := httptest.NewRequest(http.MethodPost, "/instances/term-ui/terminate", strings.NewReader("csrf="+url.QueryEscape(csrf)))
	nc.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	nr := httptest.NewRecorder()
	h.ServeHTTP(nr, nc)
	if nr.Code != http.StatusBadRequest {
		t.Fatalf("no confirm want 400 got %d", nr.Code)
	}

	// success → 303
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
	inst, err := c.Get(ctx, "term-ui")
	if err != nil || inst.Status != tasuki.StatusTerminated {
		t.Fatalf("status=%v err=%v", inst, err)
	}

	// detail after: no form
	dr2 := httptest.NewRecorder()
	h.ServeHTTP(dr2, httptest.NewRequest(http.MethodGet, "/instances/term-ui", nil))
	if strings.Contains(dr2.Body.String(), "Terminate") {
		t.Fatal("form should be hidden after terminate")
	}
}
```

- [ ] **Step 2: Run tests — expect FAIL** (no route / no form)

```bash
go test ./contrib/ui/ -count=1
```

- [ ] **Step 3: Implement `csrf.go`**

```go
package ui

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

const csrfTTL = time.Hour

func issueCSRF(secret []byte, instanceID string, now time.Time) string {
	exp := now.Add(csrfTTL).Unix()
	mac := csrfMAC(secret, instanceID, exp)
	payload := strconv.FormatInt(exp, 10) + "|" + base64.RawURLEncoding.EncodeToString(mac)
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func verifyCSRF(secret []byte, instanceID, token string, now time.Time) bool {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return false
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 2 {
		return false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || now.Unix() > exp {
		return false
	}
	mac, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	want := csrfMAC(secret, instanceID, exp)
	return hmac.Equal(mac, want)
}

func csrfMAC(secret []byte, instanceID string, exp int64) []byte {
	var expBuf [8]byte
	binary.BigEndian.PutUint64(expBuf[:], uint64(exp))
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(instanceID))
	m.Write([]byte{'\n'})
	m.Write(expBuf[:])
	return m.Sum(nil)
}
```

(Adjust encoding if simpler string MAC is preferred; tests only care about round-trip + rejection.)

- [ ] **Step 4: Update `handler.go`**

- `server` gains `secret []byte` (32 random bytes in `NewHandler` via `crypto/rand`)
- `detailPage` gains `CanTerminate bool`, `CSRFToken string`
- Set `CanTerminate = (inst.Status == tasuki.StatusRunning)`; if true, `CSRFToken = issueCSRF(...)`
- Register `POST /instances/{id}/terminate`
- Handler: `ParseForm`; `verifyCSRF` → 403; `r.Form.Get("confirm") == ""` → 400; `Terminate`; on success `http.Redirect(..., 303)`; on missing Get → 404; other errors → render detail Error with 500

- [ ] **Step 5: Update `detail.html`**

When `.CanTerminate`, after meta:

```html
{{if .CanTerminate}}
<form method="post" action="/instances/{{.ID}}/terminate" class="terminate">
  <input type="hidden" name="csrf" value="{{.CSRFToken}}">
  <label><input type="checkbox" name="confirm" value="1" required> I understand this cannot be undone</label>
  <button type="submit">Terminate</button>
</form>
{{end}}
```

Add minimal danger button CSS (e.g. red text/border).

- [ ] **Step 6: README** — change “読み取り専用” to note that running instances can be terminated from detail (confirm + CSRF); still no auth.

- [ ] **Step 7: Tests pass**

```bash
go test ./contrib/ui/ -count=1
```

- [ ] **Step 8: Commit `[skip ci]`, PR, merge**

---

## Acceptance checklist

- [ ] POST terminate + HMAC CSRF
- [ ] Confirm required; form only when running
- [ ] 303 on success
- [ ] httptest + README
- [ ] No engine API changes
