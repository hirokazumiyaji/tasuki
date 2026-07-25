# M5 Journal Size Warning + ContinueAsNew Guidance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Prefer **inline execution** (no subagents). One commit + one PR per task; merge to `main` before the next.
>
> **CI:** Every commit message AND every merge commit subject MUST include `[skip ci]`.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Warn (log + metric) when a workflow journal exceeds a configurable size threshold, and document ContinueAsNew guidance for truncating history.

**Architecture:** In `handleWorkflow`, after load and before `engine.RunAt`, compare `len(state.Journal)` to `WorkerOptions.JournalWarnThreshold` (`0` → default `10000`, `<0` → disabled). On exceed: slog Warn + `tasuki.workflow.journal_warnings` counter. No hard fail. Docs/README describe threshold + ContinueAsNew ops guidance.

**Tech Stack:** Go, slog, OpenTelemetry metrics (`observability` package).

**Spec:** [docs/superpowers/specs/2026-07-26-m5-journal-warn-design.md](../specs/2026-07-26-m5-journal-warn-design.md)

## Global Constraints

- Warn-only; never stuck/fail solely due to journal size
- Count persisted journal only (`len(state.Journal)`), not inbox about to ingest
- At most one warn per workflow task when over threshold
- Every commit / merge subject includes `[skip ci]`
- Do not change ContinueAsNew runtime semantics

---

## File structure

| Path | Responsibility |
|---|---|
| `options.go` | `JournalWarnThreshold` + defaults |
| `observability/metrics.go` | `journal_warnings` counter + helper |
| `observability/metrics_test.go` | Non-nil counter check |
| `worker.go` | Check + log + metric in `handleWorkflow` |
| `worker_journal_warn_test.go` | Integration tests (warn / disable) |
| `docs/02-architecture.md` | Update §履歴の肥大 |
| `docs/03-api.md` | Threshold semantics + ContinueAsNew tip |
| `docs/05-observability.md` | Log + metric rows |
| `docs/04-plan.md` | Risk row update |
| `README.md` | Short sticky/observability note |
| `.claude/tasks/todo.md` | Checklist |

---

### Task 1: Spec + Plan

**Files:**
- Create: `docs/superpowers/specs/2026-07-26-m5-journal-warn-design.md`
- Create: `docs/superpowers/plans/2026-07-26-m5-journal-warn.md` (this file)
- Modify: `.claude/tasks/todo.md`

```markdown
# tasuki M5 Journal Warn + ContinueAsNew Guidance

## タスク

- [x] Task 1: Spec + Plan
- [ ] Task 2: Threshold + warn (options, metrics, worker, tests)
- [ ] Task 3: Docs + README
```

- [ ] Set spec **Status:** `Approved`
- [ ] Commit + PR `m5/task-1-journal-warn-docs`

```bash
git commit -m "$(cat <<'EOF'
Add journal warn design and plan. [skip ci]

EOF
)"
```

---

### Task 2: Threshold + warn (code + tests)

**Files:**
- Modify: `options.go`, `observability/metrics.go`, `observability/metrics_test.go`, `worker.go`
- Create: `worker_journal_warn_test.go`

- [ ] **Step 1: Failing test** — create `worker_journal_warn_test.go`:

```go
package tasuki_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_JournalWarnThreshold(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         time.Millisecond,
		JournalWarnThreshold: 3,
		Logger:               log,
	})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
		for i := 0; i < 5; i++ {
			if _, err := workflow.SideEffect(wctx, func() int { return i }); err != nil {
				return 0, err
			}
		}
		if err := workflow.Sleep(wctx, time.Millisecond); err != nil {
			return 0, err
		}
		return 1, nil
	}, tasuki.WithName("fat"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	if _, err := tasuki.Start(ctx, c, "fat", struct{}{}, tasuki.WithID("fat-1")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "journal size warning") {
			return
		}
		info, err := c.Get(ctx, "fat-1")
		if err == nil && info.Status == "completed" {
			t.Fatalf("completed without warn; log=%s", buf.String())
		}
		b.SetNow(b.Now().Add(time.Second))
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for journal warn; log=%s", buf.String())
}

func TestWorker_JournalWarnThreshold_Disabled(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{
		PollInterval:         time.Millisecond,
		JournalWarnThreshold: -1,
		Logger:               log,
	})
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
		for i := 0; i < 5; i++ {
			if _, err := workflow.SideEffect(wctx, func() int { return i }); err != nil {
				return 0, err
			}
		}
		return 1, nil
	}, tasuki.WithName("fat-off"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "fat-off", struct{}{}, tasuki.WithID("fat-off-1"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "fat-off-1")
		if err == nil && info.Status == "completed" {
			if strings.Contains(buf.String(), "journal size warning") {
				t.Fatalf("unexpected warn when disabled; log=%s", buf.String())
			}
			_, _ = h, info
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for completion")
}
```

Note: `memory.Backend` may expose `Now()` / `SetNow` — if `Now()` is missing, advance with a fixed `SetNow(...Add(...))` using a local `now` variable in the test loop (same pattern as other worker tests). Adjust imports if `Result` wait is preferred over `Get` polling.

- [ ] **Step 2: Run tests — expect FAIL** (no field / no warn yet)

```bash
go test . -run 'TestWorker_JournalWarnThreshold' -count=1
```

Expected: compile error or timeout / completed without warn.

- [ ] **Step 3: Options**

In `options.go`, add field and default:

```go
JournalWarnThreshold int // 0 → 10000; <0 disabled
```

In `withDefaults`:

```go
if o.JournalWarnThreshold == 0 {
	o.JournalWarnThreshold = 10000
}
```

- [ ] **Step 4: Metrics**

In `observability/metrics.go`:

```go
JournalWarnings metric.Int64Counter
```

Create counter `tasuki.workflow.journal_warnings` with description `Workflow journal size warnings`.

```go
func (m *Metrics) AddJournalWarning(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.JournalWarnings.Add(ctx, n)
}
```

Update `TestNewMetrics` to assert `m.JournalWarnings != nil`.

- [ ] **Step 5: Worker check**

In `handleWorkflow`, after inbox ingest loop builds `events`, **before** `engine.RunAt`:

```go
if th := w.opts.JournalWarnThreshold; th > 0 {
	if n := len(state.Journal); n >= th {
		w.opts.Logger.Warn("journal size warning",
			"instance_id", t.InstanceID,
			"workflow", state.Instance.Name,
			"journal_events", n,
			"threshold", th,
		)
		w.opts.Metrics.AddJournalWarning(ctx, 1)
	}
}
```

- [ ] **Step 6: Run tests — expect PASS**

```bash
go test . -run 'TestWorker_JournalWarnThreshold|TestWorker_ContinueAsNew' -count=1
go test ./observability/ -count=1
```

- [ ] **Step 7: Commit + PR** `m5/task-2-journal-warn-code`

```bash
git commit -m "$(cat <<'EOF'
Warn when workflow journal exceeds size threshold. [skip ci]

EOF
)"
```

---

### Task 3: Docs + README

**Files:**
- Modify: `docs/02-architecture.md`, `docs/03-api.md`, `docs/05-observability.md`, `docs/04-plan.md`, `README.md`

- [ ] **Step 1: `docs/02-architecture.md` §履歴の肥大への対策** — replace the sticky “初期リリースの範囲外” sentence and clarify threshold:

```markdown
### 履歴の肥大への対策

ジャーナルは追記専用のため、ループを持つ長寿命ワークフローでは際限なく伸びる。
対策は次のとおり。

- イベント数が警告しきい値（`WorkerOptions.JournalWarnThreshold`、既定 10,000。`0` は既定値、負数で無効）を超えたら、ワークフロータスク処理時にメトリクスとログで警告する。実行は止めない。
- ワークフロー側は ContinueAsNew で履歴を打ち切り、新しい入力で新インスタンスへ引き継ぐ（旧実行は `continued`、ID は `{id}~{seq}`）。
- リプレイ時のフル履歴再読は、ワーカー内のスティッキーキャッシュで削減できる（M5）。
```

- [ ] **Step 2: `docs/03-api.md`** — update WorkerOptions comment for `JournalWarnThreshold`:

```go
JournalWarnThreshold int // 0 → 既定 10000。負数で無効。超過時は Warn + メトリクスのみ
```

Near `ContinueAsNew` row or after the API table, add a short note:

> 長寿命・ループするワークフローは、イベント数が数千〜1万付近になったら `ContinueAsNew` で履歴を打ち切ることを推奨する（既定の警告しきい値と揃える）。警告自体は実行を止めない。

- [ ] **Step 3: `docs/05-observability.md`** — add rows:

| journal size warning | Warn | `instance_id`, `workflow`, `journal_events`, `threshold` |

| `tasuki.workflow.journal_warnings` | Counter | ジャーナル件数警告の回数 |

- [ ] **Step 4: `docs/04-plan.md` risk row** — change 対策 cell to note M5 delivered warn + guidance + sticky:

`警告しきい値（実装済）・ContinueAsNew 指針（文書化）・スティッキーキャッシュ（M5）`

- [ ] **Step 5: `README.md`** — after sticky sentence (~line 16), add:

```markdown
ジャーナル件数が `JournalWarnThreshold`（既定 10000、負数で無効）以上のとき Worker は Warn ログとメトリクスを出す。長寿命ワークフローは `workflow.ContinueAsNew` で履歴を打ち切る（[docs/02-architecture.md](docs/02-architecture.md)、[docs/03-api.md](docs/03-api.md)）。
```

- [ ] **Step 6: Commit + PR** `m5/task-3-journal-warn-docs-body`

```bash
git commit -m "$(cat <<'EOF'
Document journal size warnings and ContinueAsNew guidance. [skip ci]

EOF
)"
```

---

## Spec coverage checklist

| Spec item | Task |
|---|---|
| `JournalWarnThreshold` defaults | Task 2 |
| Worker check + Warn attrs | Task 2 |
| `tasuki.workflow.journal_warnings` | Task 2 |
| Docs/README guidance | Task 3 |
| Tests warn / disable | Task 2 |
| No ContinueAsNew semantic change | Task 2 (regression only) |
