# M5 Contrib UI Signal Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [2026-07-25-m5-contrib-ui-terminate-design.md](./2026-07-25-m5-contrib-ui-terminate-design.md)  
**Decisions:** Signal form on detail page; name + JSON payload; reuse HMAC CSRF (Approach A); no confirm checkbox; no engine API changes.

## Goal

Let operators **signal** a running instance from the contrib UI detail page, reusing the existing CSRF machinery.

## Non-goals

- Cancel, Start, schedule mutations
- Confirm checkbox (Terminate-only)
- Auth / TLS
- List-page signaling
- Engine / Backend API changes

## Layout

| Path | Role |
|---|---|
| `contrib/ui/handler.go` | `POST /instances/{id}/signal`; populate Signal CSRF on detail |
| `contrib/ui/templates/detail.html` | Signal form when `CanSignal` |
| `contrib/ui/handler_test.go` | httptest for CSRF / validation / success |
| `contrib/ui/csrf.go` | Unchanged (reuse `issueCSRF` / `verifyCSRF`) |

## CSRF

Same as Terminate: process-local HMAC secret; token bound to `instance_id` + expiry. One token issued on detail GET may be used for either form (same secret + id). Invalid/expired → **403**.

## HTTP routes

| Method | Path | Behavior |
|---|---|---|
| GET | `/instances/{id}` | If `status == running`, set `CanTerminate`, `CanSignal`, and shared `CSRFToken` |
| POST | `/instances/{id}/signal` | Verify CSRF → validate name + JSON → `Client.Signal` → **303** to detail |

Form fields:

| Name | Required | Meaning |
|---|---|---|
| `csrf` | yes | HMAC token |
| `name` | yes | Signal name (non-empty after trim) |
| `payload` | no | JSON text; empty → pass `nil` to `Client.Signal` (codec encodes `null`) |

Validation:

- Missing/invalid CSRF → **403**
- Empty name → **400**
- Non-empty payload that fails `json.Valid` / unmarshal → **400**
- `Signal` / load error → re-render detail with `Error`, HTTP 500; missing instance → 404

## UI

Below Terminate form (or beside it): when running, show Signal form:

- Text input: signal name
- Textarea: payload JSON (placeholder e.g. `{}` or empty)
- Submit: “Signal”

No confirm checkbox. Reuse light theme.

## Data / API

`Client.Signal(ctx, id, name, payload)` only. Payload type: `nil` if empty field; otherwise unmarshaled `any` (or validated `json.RawMessage`) before Signal so the client codec marshals once.

## Verification

httptest with memory backend:

- GET running detail includes Signal form + csrf
- POST bad csrf → 403
- POST empty name → 400
- POST invalid JSON payload → 400
- POST `name=ping` + `payload={"ok":true}` → **303**; `LoadWorkflowHead` inbox contains `signal_received` named `ping`
- After terminate (or non-running), Signal form hidden

## Docs

README: mention Signal form alongside Terminate.

## Acceptance

- [x] `POST /instances/{id}/signal` with CSRF
- [x] name required; empty payload → nil; invalid JSON → 400
- [x] Form only when `running`
- [x] 303 on success; inbox event via `LoadWorkflowHead`
- [x] httptest + README
- [x] No engine API changes

## Follow-ups

- Cancel button
- Auth for shared deployments
