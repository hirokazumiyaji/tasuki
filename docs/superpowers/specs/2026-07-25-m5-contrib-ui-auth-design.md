# M5 Contrib UI Shared-Secret Auth Design

**Date:** 2026-07-25  
**Status:** Approved (autonomous)  
**Parent:** contrib UI follow-ups (Terminate/Signal/Cancel need auth for shared deploy)

## Goal

Optional shared-secret auth for `contrib/ui` so mutations are not open when the UI is exposed on a network.

## Non-goals

- Multi-user accounts, OAuth, TLS termination
- Per-route public/private split (all routes protected when enabled)
- CSRF replacement (HMAC CSRF remains)

## Approach

**Optional token.** Empty token → current open behavior (local default).

When configured:

- Accept `Authorization: Bearer <token>` **or** HTTP Basic (password = token; username ignored)
- On failure: **401** + `WWW-Authenticate: Basic realm="tasuki"`
- Compare with `subtle.ConstantTimeCompare`

## API

```go
func NewHandler(c *tasuki.Client, opts ...Option) http.Handler
func WithToken(token string) Option
```

Existing `NewHandler(c)` callers unchanged.

## CLI

```text
TASUKI_UI_TOKEN   # preferred
-token            # optional flag override
```

Log whether auth is enabled (not the token).

## Verification

httptest:

- No token: open access (existing tests)
- With token: no header → 401
- Bearer ok → 200
- Basic ok → 200
- Wrong token → 401

## Docs

README: document `TASUKI_UI_TOKEN` / `-token`.

## Acceptance

- [ ] WithToken + middleware
- [ ] CLI env/flag
- [ ] Tests + README
