# Quality Sprint Implementation Plan

> **For agentic workers:** Inline execution preferred (repo convention). One commit + one PR per task. Include `[skip ci]` in commit/merge subjects **except** Task 2 (CI workflow), which must run Actions. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise post-M6 quality via docs freshness, multi-backend CI, cover reporting (no gate), targeted tests, bench memo, and design acceptance checkbox cleanup.

**Architecture:** Vertical tracks from [quality-sprint design](../specs/2026-07-29-quality-sprint-design.md). Each track is independently mergeable; coverage fill uses package-scoped PRs with before/after notes.

**Tech Stack:** Go 1.24, GitHub Actions, docker-compose service images, existing `cmd/bench` / `backendtest`.

**Spec:** [docs/superpowers/specs/2026-07-29-quality-sprint-design.md](../specs/2026-07-29-quality-sprint-design.md)

---

## File map

| Path | Role |
|---|---|
| `README.md` | Status line: M5/M6 shipped; quality next |
| `docs/04-plan.md` | “次のアクション” → quality sprint |
| `.github/workflows/ci.yml` | Backend matrix + chaos jobs + cover job |
| `observability/metrics_test.go` | Call nil-safe + non-nil metric helpers |
| `backend/helpers_test.go` (new) or extend existing `*_test.go` | `InboxBatchLimit`, memo/SA helpers |
| `internal/engine/query_test.go` (new), `run_update_test.go` (new) | Package-local `RunQuery` / `ContinueUpdates` / nil ctx |
| `internal/backendopen/open_test.go` | Missing-env cases for remaining backends |
| `docs/06-bench-baseline.md` (new) | Memory bench repro + one result memo |
| `docs/superpowers/specs/*.md` | Mark implemented acceptance `[x]` |

---

### Task 1: Docs freshness

**Files:**
- Modify: `README.md` (ステータス section)
- Modify: `docs/04-plan.md` (次のアクション)

- [ ] **Step 1: Update README status opening**

Replace lines 12–13 of `README.md`:

```markdown
M4（バックエンド拡充）完了。PostgreSQL / SQLite / MySQL·MariaDB·TiDB / Spanner / DynamoDB / Firestore が適合・カオス可能な状態。
M5（性能と拡張）と M6（Query / Signal dedupe / Nack / SearchAttributes / Memo / LocalActivity / StartToClose / Update / SignalBatch など）は実装済。
現在は品質スプリント（CI マトリクス・カバレッジ計測・ドキュメント鮮度）。詳細は [docs/superpowers/specs/2026-07-29-quality-sprint-design.md](docs/superpowers/specs/2026-07-29-quality-sprint-design.md)。
M5 のベンチマーク基盤: `go run ./cmd/bench`（memory / postgres / sqlite）。
```

Keep the feature bullets below unchanged.

- [ ] **Step 2: Update `docs/04-plan.md` next actions**

Replace the 「次のアクション」 section with:

```markdown
## 次のアクション

1. 品質スプリントを実施する（[quality-sprint design](superpowers/specs/2026-07-29-quality-sprint-design.md) / [plan](superpowers/plans/2026-07-29-quality-sprint.md)）
2. 完了後、リリース準備（タグ・CHANGELOG・公開 README 整備）を検討する
```

- [ ] **Step 3: Commit + PR + merge**

Branch: `quality/task-1-docs-freshness`

```bash
git add README.md docs/04-plan.md
git commit -m "$(cat <<'EOF'
Refresh README and plan for post-M6 quality. [skip ci]

EOF
)"
```

PR title/body: docs-only. Merge subject includes `[skip ci]`.

---

### Task 2: CI backend matrix (+ chaos)

**Files:**
- Modify: `.github/workflows/ci.yml`

**Note:** Do **not** put `[skip ci]` in this commit subject — Actions must run.

- [ ] **Step 1: Replace workflow with split jobs**

Use this structure (adjust image tags only if `docker-compose.yml` differs):

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  root:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_USER: tasuki
          POSTGRES_PASSWORD: tasuki
          POSTGRES_DB: tasuki
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U tasuki -d tasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 15
    env:
      TASUKI_POSTGRES_DSN: postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test root
        run: go test ./... -race -count=1 -timeout 10m

  backend-sqlite:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test sqlite
        working-directory: backend/sqlite
        run: go test ./... -count=1 -timeout 120s

  backend-postgres:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_USER: tasuki
          POSTGRES_PASSWORD: tasuki
          POSTGRES_DB: tasuki
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U tasuki -d tasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 15
    env:
      TASUKI_POSTGRES_DSN: postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test postgres
        working-directory: backend/postgres
        run: go test ./... -count=1 -timeout 120s

  backend-mysql:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.4
        env:
          MYSQL_USER: tasuki
          MYSQL_PASSWORD: tasuki
          MYSQL_DATABASE: tasuki
          MYSQL_ROOT_PASSWORD: root
        ports:
          - 3306:3306
        options: >-
          --health-cmd "mysqladmin ping -h 127.0.0.1 -utasuki -ptasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 30
    env:
      TASUKI_MYSQL_DSN: tasuki:tasuki@tcp(127.0.0.1:3306)/tasuki?parseTime=true&loc=UTC
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test mysql
        working-directory: backend/mysql
        run: go test ./... -count=1 -timeout 180s

  backend-spanner:
    runs-on: ubuntu-latest
    services:
      spanner:
        image: gcr.io/cloud-spanner-emulator/emulator:1.5.41
        ports:
          - 9010:9010
          - 9020:9020
    env:
      SPANNER_EMULATOR_HOST: localhost:9010
      TASUKI_SPANNER_DSN: projects/tasuki/instances/tasuki/databases/tasuki
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test spanner
        working-directory: backend/spanner
        run: go test ./... -count=1 -timeout 180s

  backend-dynamodb:
    runs-on: ubuntu-latest
    services:
      dynamodb:
        image: amazon/dynamodb-local:2.5.2
        ports:
          - 8000:8000
    env:
      TASUKI_DYNAMODB_ENDPOINT: http://localhost:8000
      AWS_ACCESS_KEY_ID: local
      AWS_SECRET_ACCESS_KEY: local
      AWS_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Test dynamodb
        working-directory: backend/dynamodb
        run: go test ./... -count=1 -timeout 180s

  backend-firestore:
    runs-on: ubuntu-latest
    services:
      firestore:
        image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
        ports:
          - 8086:8086
        options: >-
          --health-cmd "curl -f http://localhost:8086 || exit 1"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 30
    env:
      FIRESTORE_EMULATOR_HOST: localhost:8086
      TASUKI_FIRESTORE_PROJECT: tasuki
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Start firestore emulator
        # If healthcheck on gcloud image is flaky, start via compose-style command in a step instead:
        # docker run is awkward in GHA services; prefer command override on the service:
        run: echo "emulator via service"
      - name: Test firestore
        working-directory: backend/firestore
        run: go test ./... -count=1 -timeout 180s

  chaos-postgres:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_USER: tasuki
          POSTGRES_PASSWORD: tasuki
          POSTGRES_DB: tasuki
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U tasuki -d tasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 15
    env:
      TASUKI_POSTGRES_DSN: postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Chaos postgres
        run: go test ./chaos/ -run TestChaos_KillWorkers$ -count=1 -timeout 5m

  chaos-mysql:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.4
        env:
          MYSQL_USER: tasuki
          MYSQL_PASSWORD: tasuki
          MYSQL_DATABASE: tasuki
          MYSQL_ROOT_PASSWORD: root
        ports:
          - 3306:3306
        options: >-
          --health-cmd "mysqladmin ping -h 127.0.0.1 -utasuki -ptasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 30
    env:
      TASUKI_MYSQL_DSN: tasuki:tasuki@tcp(127.0.0.1:3306)/tasuki?parseTime=true&loc=UTC
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Chaos mysql
        run: go test ./chaos/ -run TestChaos_KillWorkers_MySQL -count=1 -timeout 5m

  # Similarly: chaos-spanner, chaos-dynamodb, chaos-firestore with the same env as backend-* jobs
  # and -run matching TestChaos_KillWorkers_Spanner / _DynamoDB / _Firestore.
```

**Firestore service caveat:** The compose file uses `command: gcloud beta emulators firestore start --host-port=0.0.0.0:8086`. Mirror that on the GHA service:

```yaml
      firestore:
        image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
        ports:
          - 8086:8086
        options: --health-cmd "bash -c 'echo > /dev/tcp/127.0.0.1/8086'" --health-interval 5s --health-retries 30
```

If GHA `services` does not support `command`, use a step that runs the emulator container via `docker run -d` before tests (document the chosen approach in the PR body).

**DynamoDB Local:** default CMD may differ; match compose:  
`command: ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"]` — if unsupported on services, use `docker run` step.

- [ ] **Step 2: Open PR and wait for green**

Branch: `quality/task-2-ci-matrix`

```bash
git add .github/workflows/ci.yml
git commit -m "$(cat <<'EOF'
Expand CI to all primary backends and chaos jobs.

EOF
)"
```

Fix flaky emulator jobs until green, then merge (**no** `[skip ci]` on merge subject either, or use normal merge so Actions history is clear).

- [ ] **Step 3: If TiDB deferred** — leave a note in PR body; do not block Task 3.

---

### Task 3: Cover report (no gate)

**Files:**
- Modify: `.github/workflows/ci.yml` (add `cover` job)

- [ ] **Step 1: Add cover job**

```yaml
  cover:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_USER: tasuki
          POSTGRES_PASSWORD: tasuki
          POSTGRES_DB: tasuki
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U tasuki -d tasuki"
          --health-interval 2s
          --health-timeout 5s
          --health-retries 15
    env:
      TASUKI_POSTGRES_DSN: postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24.x"
      - name: Cover root
        run: go test ./... -coverprofile=cover.out -count=1 -timeout 10m
      - name: Summary
        run: |
          {
            echo '### Coverage (root module)'
            echo '```'
            go tool cover -func=cover.out | rg -v 'examples/|cmd/|backendtest/' || true
            echo '```'
            echo
            go tool cover -func=cover.out | tail -1
          } >> "$GITHUB_STEP_SUMMARY"
      - uses: actions/upload-artifact@v4
        with:
          name: cover-out
          path: cover.out
```

Ensure the Summary step never fails the job (`|| true` on filter; no threshold check).

- [ ] **Step 2: Commit + PR (CI must run)**

Branch: `quality/task-3-cover-report`

```bash
git commit -m "$(cat <<'EOF'
Add root coverage Job Summary and artifact.

EOF
)"
```

---

### Task 4: Observability helper coverage

**Baseline (package alone):** ~32.7%. Target: exercise all `Add*` / `RecordBacklog` / `MustNewMetrics` / nil receivers.

**Files:**
- Modify: `observability/metrics_test.go`

- [ ] **Step 1: Write failing/extending tests**

Append to `observability/metrics_test.go`:

```go
func TestMetrics_NilReceiverNoPanic(t *testing.T) {
	var m *observability.Metrics
	ctx := context.Background()
	m.AddWorkflowTask(ctx, 1)
	m.AddActivityTask(ctx, 1)
	m.AddTerminal(ctx, "completed")
	m.AddActivityRetry(ctx, 1)
	m.AddJournalWarning(ctx, 1)
	m.AddIncompatibleNack(ctx, "unknown_type")
	m.RecordBacklog(ctx, "workflow", "default", 3)
}

func TestMetrics_AddHelpers(t *testing.T) {
	m := observability.MustNewMetrics()
	ctx := context.Background()
	m.AddWorkflowTask(ctx, 1)
	m.AddActivityTask(ctx, 2)
	m.AddTerminal(ctx, "failed")
	m.AddActivityRetry(ctx, 1)
	m.AddJournalWarning(ctx, 1)
	m.AddIncompatibleNack(ctx, "determinism")
	m.RecordBacklog(ctx, "activity", "default", 9)
}
```

Add `"context"` to imports.

- [ ] **Step 2: Run**

```bash
go test ./observability/ -count=1 -cover
```

Expected: PASS; coverage clearly above ~32%.

- [ ] **Step 3: Commit + PR**

Branch: `quality/task-4-observability-cover`

```bash
git commit -m "$(cat <<'EOF'
Cover observability metric helper methods. [skip ci]

EOF
)"
```

PR body: before/after `%` for `observability`.

---

### Task 5: Backend helper coverage

**Files:**
- Create: `backend/helpers_test.go` (or extend `backend/search_attributes_test.go` + new memo/inbox tests)

- [ ] **Step 1: Add tests**

```go
package backend_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestInboxBatchLimit(t *testing.T) {
	if got := backend.InboxBatchLimit(backend.Capabilities{}); got != backend.DefaultInboxBatchLimit {
		t.Fatalf("default=%d", got)
	}
	if got := backend.InboxBatchLimit(backend.Capabilities{MaxAdvancementEffects: 40}); got != 10 {
		t.Fatalf("got %d want 10", got)
	}
	if got := backend.InboxBatchLimit(backend.Capabilities{MaxAdvancementEffects: 3}); got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}

func TestMarshalSearchAttributes(t *testing.T) {
	if string(backend.MarshalSearchAttributes(nil)) != "{}" {
		t.Fatal("nil")
	}
	b := backend.MarshalSearchAttributes(map[string]string{"a": "1"})
	if string(b) != `{"a":"1"}` {
		t.Fatalf("%s", b)
	}
}

func TestHasSearchAttributesUpdate(t *testing.T) {
	if backend.HasSearchAttributesUpdate(nil) {
		t.Fatal("empty")
	}
	evs := []journal.Event{{Type: journal.TypeSearchAttributesUpdated, Payload: []byte(`{}`)}}
	if !backend.HasSearchAttributesUpdate(evs) {
		t.Fatal("want true")
	}
}

func TestHasMemoUpdateAndLast(t *testing.T) {
	if backend.HasMemoUpdate(nil) || backend.LastMemoUpdate(nil) != nil {
		t.Fatal("empty")
	}
	evs := []journal.Event{
		{Type: journal.TypeMemoUpdated, Payload: []byte(`{"n":"1"}`)},
		{Type: journal.TypeActivityScheduled},
		{Type: journal.TypeMemoUpdated, Payload: []byte(`{"n":"2"}`)},
	}
	if !backend.HasMemoUpdate(evs) {
		t.Fatal("want has")
	}
	got := backend.LastMemoUpdate(evs)
	if got["n"] != "2" {
		t.Fatalf("%#v", got)
	}
	bad := []journal.Event{{Type: journal.TypeMemoUpdated, Payload: []byte(`not-json`)}}
	if backend.LastMemoUpdate(bad) != nil {
		t.Fatal("bad payload")
	}
}
```

- [ ] **Step 2: Run**

```bash
go test ./backend/ -count=1 -cover
```

Expected: PASS; `InboxBatchLimit` / memo / marshal helpers no longer 0%.

- [ ] **Step 3: Commit + PR** — branch `quality/task-5-backend-helpers`, `[skip ci]`.

---

### Task 6: Engine package-local coverage

`RunQuery` / `ContinueUpdates` are already exercised from `workflow/*_test.go` under `go test ./...`, but `go test ./internal/engine` alone stays ~34%. Add package-local tests so the priority package cover rises when measured in isolation and in root profile attribution.

**Files:**
- Create: `internal/engine/query_test.go`
- Create: `internal/engine/continue_updates_test.go`

- [ ] **Step 1: `query_test.go`**

```go
package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRunQuery_Happy(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunQuery(events, time.Time{}, "n", nil, func(ctx *workflow.Context) (any, error) {
		workflow.SetQueryHandler(ctx, "n", func(_ struct{}) (int, error) { return 42, nil })
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if res.Err != nil || res.Stuck || string(res.Payload) != "42" {
		t.Fatalf("%+v", res)
	}
}

func TestRunQuery_Unknown(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunQuery(events, time.Time{}, "missing", nil, func(ctx *workflow.Context) (any, error) {
		_ = workflow.Sleep(ctx, 0)
		return 0, nil
	})
	if !errors.Is(res.Err, workflow.ErrUnknownQuery) {
		t.Fatalf("%v", res.Err)
	}
}
```

- [ ] **Step 2: `continue_updates_test.go`**

```go
package engine_test

import (
	"encoding/json"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestContinueUpdates_NilContext(t *testing.T) {
	res := engine.ContinueUpdates(nil)
	if res.Suspended || res.Stuck || res.Err != nil {
		t.Fatalf("%+v", res)
	}
}

func TestContinueUpdates_AfterRun(t *testing.T) {
	req, _ := json.Marshal(map[string]any{"id": "u1", "input": 3})
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeUpdateRequested, Name: "inc", Payload: req},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		workflow.SetUpdateHandler(ctx, "inc", func(ctx *workflow.Context, n int) (int, error) {
			return n + 1, nil
		})
		return "ok", nil
	})
	if res.WorkflowContext() == nil {
		t.Fatal("nil ctx")
	}
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Stuck || ures.Suspended {
		t.Fatalf("%+v", ures)
	}
	cmds := res.WorkflowContext().NewCommands()
	if len(cmds) < 2 {
		t.Fatalf("%+v", cmds)
	}
}

func TestContinueUpdates_NoPending(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		return "done", nil
	})
	ures := engine.ContinueUpdates(res.WorkflowContext())
	if ures.Suspended || len(ures.NewCommands) != 0 {
		t.Fatalf("%+v", ures)
	}
}
```

- [ ] **Step 3: Run**

```bash
go test ./internal/engine/ -count=1 -cover -timeout 60s
```

Expected: PASS; package cover well above 33%.

- [ ] **Step 4: Commit + PR** — `quality/task-6-engine-cover`, `[skip ci]`, note before/after.

---

### Task 7: backendopen missing-env coverage

**Files:**
- Modify: `internal/backendopen/open_test.go`

- [ ] **Step 1: Add tests for remaining required env errors**

```go
func TestOpen_Memory(t *testing.T) {
	b, closer, err := backendopen.Open(context.Background(), "memory", backendopen.Options{})
	if err != nil || b == nil {
		t.Fatalf("%v %v", b, err)
	}
	closer()
}

func TestOpen_PostgresMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_POSTGRES_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "postgres", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_POSTGRES_DSN") {
		t.Fatalf("%v", err)
	}
}

func TestOpen_MySQLMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_MYSQL_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "mysql", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_MYSQL_DSN") {
		t.Fatalf("%v", err)
	}
}

func TestOpen_SpannerMissingEnv(t *testing.T) {
	t.Setenv("TASUKI_SPANNER_DSN", "")
	_, _, err := backendopen.Open(context.Background(), "spanner", backendopen.Options{})
	if err == nil || !strings.Contains(err.Error(), "TASUKI_SPANNER_DSN") {
		t.Fatalf("%v", err)
	}
}
```

Do **not** require live cloud stores in this task (env-missing paths only). Live Open paths remain covered by sqlite temp test + optional future CI with services.

- [ ] **Step 2: Run** `go test ./internal/backendopen/ -count=1 -cover`

- [ ] **Step 3: Commit + PR** — `quality/task-7-backendopen-cover`, `[skip ci]`.

---

### Task 8: Bench baseline memo

**Files:**
- Create: `docs/06-bench-baseline.md`
- Modify: `README.md` (one link under bench section)

- [ ] **Step 1: Run memory bench once and capture output**

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms
```

Paste the human-readable result into the doc (date = run day).

- [ ] **Step 2: Write `docs/06-bench-baseline.md`**

```markdown
# Bench baseline (memory)

再現手順（SLO ゲートなし。比較用メモ）:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms
```

Postgres（手動）:

```bash
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./cmd/bench -backend=postgres -instances=200 -workers=4
```

## Captured result (memory)

- Date: YYYY-MM-DD
- Host: (optional)
- Command: (above)
- Result: (paste `FormatHuman` output)
```

- [ ] **Step 3: Link from README** near existing bench commands:

```markdown
基準メモ: [docs/06-bench-baseline.md](docs/06-bench-baseline.md)。
```

- [ ] **Step 4: Commit + PR** — `quality/task-8-bench-baseline`, `[skip ci]`.

---

### Task 9: Spec checkbox cleanup — M5 notify / hub / cross-process

**Files:** design specs under `docs/superpowers/specs/` for notify, hub, cross-process, journal-warn, backlog-metrics (all shipped).

- [ ] **Step 1:** For each file whose acceptance items are already true in `main`, change `- [ ]` → `- [x]`.

Start with (all implemented per README / plan):

- `2026-07-26-m5-notify-hub-design.md`
- `2026-07-26-m5-cross-process-notify-design.md`
- `2026-07-26-m5-journal-warn-design.md`
- `2026-07-26-m5-backlog-metrics-design.md`
- `2026-07-25-m5-*-notify-design.md` (memory/sqlite/mysql/spanner/dynamodb-firestore)
- `2026-07-26-m6-activity-heartbeat-design.md`

Leave unchecked anything still false after reading the box text.

- [ ] **Step 2: Commit + PR** — `quality/task-9-spec-checkboxes-m5`, `[skip ci]`.

---

### Task 10: Spec checkbox cleanup — remaining M5 / M4 leftovers

- [ ] **Step 1:** Same treatment for remaining unchecked files that are fully shipped (claim-limit, sticky-cache, activity/workflow parallel, commit-batch, contrib-ui slices, shared-backend-open, M4 store designs, etc.).

- [ ] **Step 2:** Mark quality-sprint design acceptance boxes as work completes (or leave until sprint end).

- [ ] **Step 3: Commit + PR** — `quality/task-10-spec-checkboxes-rest`, `[skip ci]`.

---

### Task 11 (optional stretch): Thin workflow / more engine edges

Only if cover summary still shows easy wins:

- Invalid JSON / empty payload branches in helpers already covered
- Additional `workflow` error paths not covered by existing tests

Skip if Tasks 1–10 meet acceptance in the design doc.

---

## Spec coverage checklist

| Spec requirement | Task |
|---|---|
| README / `04-plan` freshness | 1 |
| CI backends sqlite…firestore | 2 |
| Chaos jobs with env set | 2 |
| Cover Job Summary + artifact, no fail gate | 3 |
| Priority package tests + before/after | 4–7 |
| Memory bench memo + repro | 8 |
| Implemented design checkboxes | 9–10 |
| TiDB optional | deferred (Task 2 note) |

## Placeholder / consistency self-review

- No TBD steps; CI firestore/dynamodb service command caveats called out with fallback
- Commit `[skip ci]` rule matches design (exception Task 2/3)
- Helper APIs match current `backend` / `engine` / `observability` packages

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-29-quality-sprint.md`.

**Two execution options:**

1. **Subagent-Driven** — fresh subagent per task + review between tasks  
2. **Inline Execution** (recommended for this repo) — execute in this session with checkpoints

Which approach?
