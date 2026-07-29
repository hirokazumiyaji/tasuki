# M5 Contrib UI Terminate Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [2026-07-24-m5-contrib-ui-design.md](./2026-07-24-m5-contrib-ui-design.md), FR-15  
**Decisions:** Terminate only (no Signal); detail-page form with confirm checkbox; HMAC CSRF (Approach B); no engine API changes.

## Goal

Extend the contrib read-only Web UI so operators can **terminate** a running instance from the detail page, with an explicit confirm step and a lightweight CSRF check.

## Non-goals

- Signal, Cancel, Start, or schedule mutations
- Auth / TLS / multi-user sessions
- List-page bulk terminate
- CSRF cookie / shared session store
- CLI backend expansion
- Separate `contrib` `go.mod`

## Layout

Unchanged package layout under `contrib/ui`. New helpers may live in `handler.go` or a small `csrf.go` beside it.

| Path | Role |
|---|---|
| `contrib/ui/handler.go` | Add `POST /instances/{id}/terminate`; wire CSRF + Terminate |
| `contrib/ui/csrf.go` (optional) | Issue / verify HMAC tokens |
| `contrib/ui/templates/detail.html` | Terminate form when `CanTerminate` |
| `contrib/ui/handler_test.go` | httptest coverage for CSRF / confirm / success |

## CSRF

- `NewHandler` generates a random 32-byte secret (process-local).
- On detail GET, when the instance is terminable, issue a token:
  - Payload: `instance_id` + Unix expiry (e.g. 1 hour).
  - Signature: HMAC-SHA256 over that payload with the handler secret.
  - Form field: opaque string encoding payload + MAC (implementation detail).
- On POST, verify: MAC valid, not expired, `instance_id` matches path `{id}`.
- Failure → **403** (do not call `Terminate`).
- No cookies, no server-side token store.

## HTTP routes

| Method | Path | Behavior |
|---|---|---|
| GET | `/` | Unchanged (list) |
| GET | `/instances/{id}` | Unchanged data load; if `status == running`, set `CanTerminate` and `CSRFToken` for the form |
| POST | `/instances/{id}/terminate` | Parse form: `csrf`, `confirm`. CSRF fail → 403. Missing/invalid `confirm` → 400. Else `Client.Terminate`; success → **303** to `/instances/{id}`; `Terminate` / load error → re-render detail with `Error`, HTTP 500; missing instance → 404 |

Form fields:

| Name | Required | Meaning |
|---|---|---|
| `csrf` | yes | HMAC token from GET |
| `confirm` | yes | Must be present (e.g. checkbox value `1` / `on`) |

## UI

- On detail, below meta (above journal): when `CanTerminate`, show a form:
  - Checkbox label: e.g. “I understand this cannot be undone”
  - Submit: “Terminate”
- After terminate (or any non-`running` status), form is hidden.
- Reuse existing light theme; danger styling for the button is fine (no purple gradients).

## Data / API

Reuse `Client.Terminate` only. No new Backend methods. No changes to core `tasuki` Client surface beyond what already exists.

## CLI

No new flags. Existing `tasuki-ui` serves the updated handler.

## Verification

httptest with in-memory client:

- GET detail for running instance includes form + csrf field
- POST without csrf / bad csrf → 403, status still running
- POST without confirm → 400, status still running
- POST with valid csrf + confirm → 303, subsequent GET shows `terminated`, no form
- POST for missing id → 404 (or Terminate error surfaced consistently)

## Docs

README “閲覧 UI” section: note that detail page can terminate running instances (confirm + CSRF); still no auth.

## Acceptance

- [x] `POST /instances/{id}/terminate` with HMAC CSRF
- [x] Confirm checkbox required
- [x] Form only when `status == running`
- [x] 303 redirect on success
- [x] httptest coverage above
- [x] README note
- [x] No engine / Backend API changes

## Follow-ups

- Signal form (similar CSRF pattern)
- Auth for shared deployments
- More CLI backends
