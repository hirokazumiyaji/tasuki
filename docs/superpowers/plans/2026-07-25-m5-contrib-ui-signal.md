# M5 Contrib UI Signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Add Signal form (name + JSON payload) on contrib UI detail page with shared HMAC CSRF.

**Architecture:** Reuse `issueCSRF`/`verifyCSRF`; `POST /instances/{id}/signal` validates name/JSON then `Client.Signal`; 303 redirect.

**Tech Stack:** net/http, encoding/json, existing CSRF helpers, `tasuki.Client.Signal`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-contrib-ui-signal-design.md](../specs/2026-07-25-m5-contrib-ui-signal-design.md)

## Global Constraints

- Signal only (no Cancel); contrib UI only; no engine API changes
- Form only when `status == running`; reuse CSRF; `[skip ci]`; one PR per task

## File map

| File | Responsibility |
|---|---|
| `contrib/ui/handler.go` | POST signal + `CanSignal` on detail |
| `contrib/ui/templates/detail.html` | Signal form |
| `contrib/ui/handler_test.go` | httptest + inbox assert |
| `README.md` | Document Signal |

---

### Task 1: Spec + Plan docs

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-contrib-ui-signal-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-contrib-ui-signal.md`
- Modify: `.claude/tasks/todo.md`

- [ ] Commit + PR + merge `[skip ci]`

---

### Task 2: Signal route + form + tests + README

**Files:**
- Modify: `contrib/ui/handler.go`
- Modify: `contrib/ui/templates/detail.html`
- Modify: `contrib/ui/handler_test.go`
- Modify: `README.md`

- [ ] **Step 1: Failing test** `TestHandler_Signal` in `handler_test.go`:

```go
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
		t.Fatalf("want 303 got %d", or.Code)
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

func formPost(path, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}
```

- [ ] **Step 2:** `go test ./contrib/ui/ -count=1` — expect FAIL

- [ ] **Step 3: handler.go**

- `detailPage` add `CanSignal bool` (CSRFToken already shared)
- When running: `CanSignal = true` (with CanTerminate)
- Register `POST /instances/{id}/signal`
- Handler logic:

```go
func (s *server) handleSignal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	if !verifyCSRF(s.secret, id, r.Form.Get("csrf"), time.Now()) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(r.Form.Get("name"))
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	raw := strings.TrimSpace(r.Form.Get("payload"))
	var payload any
	if raw != "" {
		if !json.Valid([]byte(raw)) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}
	if err := s.client.Signal(r.Context(), id, name, payload); err != nil {
		page, code := s.buildDetail(r, id, err.Error())
		if page.NotFound {
			s.render(w, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}
```

- [ ] **Step 4: detail.html** — after Terminate form:

```html
{{if .CanSignal}}
<form method="post" action="/instances/{{.ID}}/signal" class="signal">
  <input type="hidden" name="csrf" value="{{.CSRFToken}}">
  <label>Name <input name="name" required></label>
  <label>Payload (JSON) <textarea name="payload" rows="3" placeholder="{}"></textarea></label>
  <button type="submit">Signal</button>
</form>
{{end}}
```

Minimal CSS for `.signal` (same card style as terminate).

- [ ] **Step 5: README** — mention Signal (name + JSON) on detail.

- [ ] **Step 6:** `go test ./contrib/ui/ -count=1` PASS

- [ ] **Step 7:** Commit `[skip ci]`, PR, merge

---

## Acceptance checklist

- [ ] POST signal + CSRF
- [ ] Validation + inbox assert
- [ ] README; no engine changes
