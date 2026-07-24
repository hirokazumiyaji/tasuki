# M5 Contrib UI Cancel Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [2026-07-25-m5-contrib-ui-terminate-design.md](./2026-07-25-m5-contrib-ui-terminate-design.md), [2026-07-25-m5-contrib-ui-signal-design.md](./2026-07-25-m5-contrib-ui-signal-design.md)  
**Decisions:** Cancel form mirroring Terminate (confirm + HMAC CSRF); Approach A; cooperative cancel (inbox); no engine API changes.

## Goal

Let operators request **cooperative cancel** (`Client.Cancel`) from the contrib UI detail page, with the same confirm + CSRF pattern as Terminate.

## Non-goals

- Immediate force-kill (that remains Terminate)
- Auth / TLS / bulk cancel
- Changing Cancel semantics in the engine
- Hiding the form after Cancel (instance usually stays `running` until the workflow observes cancel)

## Layout

| Path | Role |
|---|---|
| `contrib/ui/handler.go` | `POST /instances/{id}/cancel`; `CanCancel` on detail |
| `contrib/ui/templates/detail.html` | Cancel form when running |
| `contrib/ui/handler_test.go` | httptest + inbox assert |
| `contrib/ui/csrf.go` | Unchanged (reuse) |

## CSRF

Same process-local HMAC as Terminate/Signal. Shared `CSRFToken` on detail GET when `status == running`. Invalid → **403**.

## HTTP routes

| Method | Path | Behavior |
|---|---|---|
| GET | `/instances/{id}` | If running: `CanTerminate`, `CanSignal`, `CanCancel`, `CSRFToken` |
| POST | `/instances/{id}/cancel` | CSRF → confirm required → `Client.Cancel` → **303** to detail |

Form fields:

| Name | Required | Meaning |
|---|---|---|
| `csrf` | yes | HMAC token |
| `confirm` | yes | Checkbox (e.g. value `1`) |

- Missing confirm → **400**
- Cancel / load error → re-render detail with `Error`, 500; missing → 404

## UI

Near Terminate: when running, show Cancel form:

- Checkbox: e.g. “Request cooperative cancel”
- Submit: “Cancel”
- Short hint that this is cooperative (workflow may continue until it handles cancel); Terminate remains for immediate stop

Style: distinct from Terminate danger red (e.g. muted/accent border), still not purple-gradient themed.

## Data / API

`Client.Cancel(ctx, id)` only. Delivers `cancel_requested` to inbox.

## Verification

httptest + memory `LoadWorkflowHead`:

- GET running detail includes Cancel form
- POST bad csrf → 403
- POST no confirm → 400
- POST valid → **303**; inbox contains `cancel_requested`
- Instance status may still be `running`

## Docs

README: mention Cancel alongside Terminate / Signal (cooperative vs terminate).

## Acceptance

- [ ] `POST /instances/{id}/cancel` with CSRF + confirm
- [ ] Form only when `running`
- [ ] 303 on success; inbox event via `LoadWorkflowHead`
- [ ] httptest + README
- [ ] No engine API changes

## Follow-ups

- Auth for shared deployments
- CLI more backends
