# M5 Contrib UI Cancel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Add confirm + HMAC-CSRF Cancel on the contrib UI detail page (cooperative cancel via `Client.Cancel`).

**Architecture:** Mirror Terminate route/form; reuse CSRF; assert `cancel_requested` in inbox via `LoadWorkflowHead`.

**Tech Stack:** net/http, existing CSRF, `tasuki.Client.Cancel`.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-contrib-ui-cancel-design.md](../specs/2026-07-25-m5-contrib-ui-cancel-design.md)

## Global Constraints

- Cancel only (no engine changes); form when `running`; confirm + CSRF; `[skip ci]`; one PR per task

## File map

| File | Responsibility |
|---|---|
| `contrib/ui/handler.go` | POST cancel + `CanCancel` |
| `contrib/ui/templates/detail.html` | Cancel form |
| `contrib/ui/handler_test.go` | httptest |
| `README.md` | Document Cancel |

---

### Task 1: Spec + Plan docs

**Files:**
- Create: `docs/superpowers/specs/2026-07-25-m5-contrib-ui-cancel-design.md`
- Create: `docs/superpowers/plans/2026-07-25-m5-contrib-ui-cancel.md`
- Modify: `.claude/tasks/todo.md`

- [ ] Commit + PR + merge `[skip ci]`

---

### Task 2: Cancel route + form + tests + README

**Files:**
- Modify: `contrib/ui/handler.go`
- Modify: `contrib/ui/templates/detail.html`
- Modify: `contrib/ui/handler_test.go`
- Modify: `README.md`

- [ ] **Step 1: Failing test** `TestHandler_Cancel`:

```go
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
		t.Fatalf("want 303 got %d", or.Code)
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
```

- [ ] **Step 2:** `go test ./contrib/ui/ -count=1` — expect FAIL

- [ ] **Step 3: handler.go** — `CanCancel`; `POST /instances/{id}/cancel` mirroring `handleTerminate` but calling `Client.Cancel`

- [ ] **Step 4: detail.html** — Cancel form near Terminate (confirm checkbox; hint text; distinct CSS)

- [ ] **Step 5: README** — Cancel is cooperative; Terminate is immediate

- [ ] **Step 6:** tests PASS; commit `[skip ci]`; PR; merge

---

## Acceptance checklist

- [ ] POST cancel + CSRF + confirm
- [ ] Inbox `cancel_requested`; README; no engine changes
