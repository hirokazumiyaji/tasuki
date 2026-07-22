# M0 実行モデル検証 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do **not** use subagents for this plan (user preference: inline execution).

**Goal:** ジャーナル再実行方式が Go で快適に書けることを検証する。`workflow.Context` / `Execute` / `Sleep` / `runtime.Goexit` によるサスペンド、決定性違反検出、インメモリバックエンド、仮想時計（`wftest` 原型）を実装し、M0 受入条件を自動テストでグリーンにする。

**Architecture:** ワークフロー関数をタスクごとに先頭から再実行し、記録済みコマンドの結果を返すことで現在位置を復元する。未完了の待ちで `runtime.Goexit` によりゴルーチンを終了し、蓄積したコマンドをコミットする。永続化は `backend.Backend` のインメモリ実装。テストは `wftest` が仮想時計でタイマーを進める。

**Tech Stack:** Go 1.24+、標準ライブラリのみ（本体）。モジュールパス `github.com/hirokazumiyaji/tasuki`。

**Spec references:** [docs/04-plan.md](../../04-plan.md) M0、[docs/02-architecture.md](../../02-architecture.md)、[docs/03-api.md](../../03-api.md)

---

## File structure

| Path | Responsibility |
|---|---|
| `go.mod` | モジュール定義 |
| `journal/event.go` | イベント型・定数 |
| `journal/match.go` | コマンド照合（type + name） |
| `workflow/context.go` | `*workflow.Context`、リプレイカーソル、コマンド蓄積 |
| `workflow/execute.go` | `Execute` / `ExecuteAsync` / `Future.Get` |
| `workflow/sleep.go` | `Sleep` / `SleepUntil` |
| `workflow/errors.go` | `ErrCanceled` など |
| `internal/engine/executor.go` | ゴルーチン実行、Goexit 検知、決定性違反 → stuck |
| `backend/types.go` | `Backend` と関連型（M0 に必要な部分集合） |
| `backend/memory/memory.go` | インメモリバックエンド |
| `codec/json.go` | 既定 JSON Codec |
| `registry/registry.go` | ワークフロー / アクティビティ登録 |
| `wftest/env.go` | テスト環境・仮想時計・`Run` |
| `examples/m0-hello/main.go` | 動くデモ |
| `README.md` | ステータス更新 |

---

### Task 1: Module scaffold and journal event types

**Files:**
- Create: `go.mod`
- Create: `journal/event.go`
- Create: `journal/event_test.go`
- Create: `.gitignore`

- [ ] **Step 1: Write the failing test**

```go
// journal/event_test.go
package journal_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestEventTypeConstants(t *testing.T) {
	cases := []journal.Type{
		journal.TypeWorkflowStarted,
		journal.TypeActivityScheduled,
		journal.TypeTimerCreated,
		journal.TypeActivityCompleted,
		journal.TypeTimerFired,
		journal.TypeWorkflowCompleted,
		journal.TypeWorkflowFailed,
	}
	for _, typ := range cases {
		if typ == "" {
			t.Fatalf("empty event type")
		}
	}
}

func TestIsCommand(t *testing.T) {
	if !journal.TypeActivityScheduled.IsCommand() {
		t.Fatal("activity_scheduled should be command")
	}
	if journal.TypeActivityCompleted.IsCommand() {
		t.Fatal("activity_completed should not be command")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./journal/ -count=1`
Expected: FAIL（package 未定義）

- [ ] **Step 3: Write minimal implementation**

```go
// go.mod
module github.com/hirokazumiyaji/tasuki

go 1.24
```

```gitignore
# .gitignore
.worktrees/
worktrees/
*.exe
*.test
*.out
bin/
dist/
.idea/
.vscode/
*.swp
.DS_Store
```

```go
// journal/event.go
package journal

type Type string

const (
	TypeWorkflowStarted    Type = "workflow_started"
	TypeActivityScheduled  Type = "activity_scheduled"
	TypeTimerCreated       Type = "timer_created"
	TypeSideEffect         Type = "side_effect"
	TypeNowRecorded        Type = "now_recorded"
	TypeVersionMarker      Type = "version_marker"
	TypeActivityCompleted  Type = "activity_completed"
	TypeActivityFailed     Type = "activity_failed"
	TypeTimerFired         Type = "timer_fired"
	TypeSignalReceived     Type = "signal_received"
	TypeCancelRequested    Type = "cancel_requested"
	TypeWorkflowCompleted  Type = "workflow_completed"
	TypeWorkflowFailed     Type = "workflow_failed"
	TypeWorkflowCanceled   Type = "workflow_canceled"
	TypeContinuedAsNew     Type = "continued_as_new"
)

type Event struct {
	Seq     int64
	Type    Type
	Name    string // activity / signal / workflow name when applicable
	RefSeq  int64
	Payload []byte
}

func (t Type) IsCommand() bool {
	switch t {
	case TypeActivityScheduled, TypeTimerCreated, TypeSideEffect, TypeNowRecorded, TypeVersionMarker:
		return true
	default:
		return false
	}
}

func (t Type) IsCompletion() bool {
	switch t {
	case TypeActivityCompleted, TypeActivityFailed, TypeTimerFired, TypeSignalReceived, TypeCancelRequested:
		return true
	default:
		return false
	}
}

func (t Type) IsTerminal() bool {
	switch t {
	case TypeWorkflowCompleted, TypeWorkflowFailed, TypeWorkflowCanceled, TypeContinuedAsNew:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./journal/ -count=1`
Expected: PASS

- [ ] **Step 5: Commit and open PR**

```bash
git checkout -b m0/task-1-journal-events
git add go.mod .gitignore journal/
git commit -m "$(cat <<'EOF'
Add module scaffold and journal event types for M0.

EOF
)"
git push -u origin HEAD
gh pr create --title "M0 Task 1: journal event types" --body "$(cat <<'EOF'
## Summary
- Add `go.mod` for `github.com/hirokazumiyaji/tasuki`
- Define journal event types and classification helpers

## Test plan
- [x] `go test ./journal/ -count=1`

EOF
)"
```

Merge the PR into `main` before starting Task 2:

```bash
gh pr merge --merge
git checkout main && git pull
```

---

### Task 2: Command matching and determinism violation

**Files:**
- Create: `journal/match.go`
- Create: `journal/match_test.go`
- Create: `journal/errors.go`

- [ ] **Step 1: Write the failing test**

```go
// journal/match_test.go
package journal_test

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMatchCommand_OK(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeActivityScheduled, Name: "Charge"}
	if err := journal.MatchCommand(recorded, cmd); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestMatchCommand_TypeMismatch(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeTimerCreated, Name: ""}
	err := journal.MatchCommand(recorded, cmd)
	if !errors.Is(err, journal.ErrDeterminismViolation) {
		t.Fatalf("want ErrDeterminismViolation, got %v", err)
	}
}

func TestMatchCommand_NameMismatch(t *testing.T) {
	recorded := journal.Event{Type: journal.TypeActivityScheduled, Name: "Charge"}
	cmd := journal.Command{Type: journal.TypeActivityScheduled, Name: "Refund"}
	err := journal.MatchCommand(recorded, cmd)
	if !errors.Is(err, journal.ErrDeterminismViolation) {
		t.Fatalf("want ErrDeterminismViolation, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./journal/ -count=1`
Expected: FAIL（`MatchCommand` 未定義）

- [ ] **Step 3: Write minimal implementation**

```go
// journal/errors.go
package journal

import "errors"

var ErrDeterminismViolation = errors.New("determinism violation")
```

```go
// journal/match.go
package journal

import "fmt"

type Command struct {
	Type Type
	Name string
}

func MatchCommand(recorded Event, cmd Command) error {
	if recorded.Type != cmd.Type || recorded.Name != cmd.Name {
		return fmt.Errorf("%w: recorded=%s/%s got=%s/%s",
			ErrDeterminismViolation, recorded.Type, recorded.Name, cmd.Type, cmd.Name)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./journal/ -count=1`
Expected: PASS

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-2-command-matching`
Commit message: `Add journal command matching for determinism checks.`
PR title: `M0 Task 2: command matching`
Merge into `main` before Task 3.

---

### Task 3: workflow.Context replay cursor and Goexit suspend

**Files:**
- Create: `workflow/context.go`
- Create: `workflow/errors.go`
- Create: `workflow/context_test.go`
- Create: `internal/engine/run.go`
- Create: `internal/engine/run_test.go`

- [ ] **Step 1: Write the failing test for suspend**

```go
// internal/engine/run_test.go
package engine_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_SuspendsOnUnresolvedWait(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
	}
	var suspended bool
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// Schedule a timer command with no completion → must suspend
		if err := workflow.Sleep(ctx, 0); err != nil {
			return nil, err
		}
		suspended = false
		return "done", nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
	if len(res.NewCommands) != 1 || res.NewCommands[0].Type != journal.TypeTimerCreated {
		t.Fatalf("want one timer_created, got %+v", res.NewCommands)
	}
	_ = suspended
}

func TestRun_CompletesWhenJournalHasResults(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeTimerCreated},
		{Seq: 3, Type: journal.TypeTimerFired, RefSeq: 2},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		if err := workflow.Sleep(ctx, 0); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	if res.Suspended {
		t.Fatal("should not suspend")
	}
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result != "ok" {
		t.Fatalf("got %v", res.Result)
	}
	if len(res.NewCommands) != 0 {
		t.Fatalf("no new commands on pure replay, got %+v", res.NewCommands)
	}
}

func TestRun_DeferRunsOnSuspend(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	var deferred bool
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		defer func() { deferred = true }()
		_ = workflow.Sleep(ctx, 0)
		return nil, nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
	if !deferred {
		t.Fatal("defer should run on Goexit suspend")
	}
}

func TestRun_RecoverDoesNotCatchSuspend(t *testing.T) {
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("recover caught suspend sentinel: %v", r)
			}
		}()
		_ = workflow.Sleep(ctx, 0)
		return "should-not-reach", nil
	})
	if !res.Suspended {
		t.Fatal("expected suspend")
	}
}
```

Note: Task 3 introduces `Sleep` stub that always schedules/suspends when no timer completion exists. Full duration handling comes in Task 5.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/engine/ -count=1`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```go
// workflow/errors.go
package workflow

import "errors"

var ErrCanceled = errors.New("workflow canceled")
```

```go
// workflow/context.go
package workflow

import (
	"runtime"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type Context struct {
	events      []journal.Event
	cmdIndex    int // index into command events only
	commands    []journal.Event
	nextSeq     int64
	now         time.Time
	completions map[int64]journal.Event // ref_seq → completion
	canceled    bool
}

func NewContext(events []journal.Event, now time.Time) *Context {
	ctx := &Context{
		events:      events,
		commands:    nil,
		now:         now,
		completions: map[int64]journal.Event{},
		nextSeq:     1,
	}
	for _, e := range events {
		if e.Seq >= ctx.nextSeq {
			ctx.nextSeq = e.Seq + 1
		}
		if e.Type.IsCompletion() && e.RefSeq != 0 {
			ctx.completions[e.RefSeq] = e
		}
		if e.Type == journal.TypeCancelRequested {
			ctx.canceled = true
		}
	}
	return ctx
}

func (c *Context) NewCommands() []journal.Event { return c.commands }

func (c *Context) recordOrReplay(cmd journal.Command, payload []byte) (journal.Event, bool) {
	// bool = isNew
	recordedCmds := c.recordedCommands()
	if c.cmdIndex < len(recordedCmds) {
		rec := recordedCmds[c.cmdIndex]
		c.cmdIndex++
		if err := journal.MatchCommand(rec, cmd); err != nil {
			panic(determinismPanic{err: err})
		}
		return rec, false
	}
	ev := journal.Event{
		Seq:     c.nextSeq,
		Type:    cmd.Type,
		Name:    cmd.Name,
		Payload: payload,
	}
	c.nextSeq++
	c.commands = append(c.commands, ev)
	c.cmdIndex++
	return ev, true
}

func (c *Context) recordedCommands() []journal.Event {
	out := make([]journal.Event, 0)
	for _, e := range c.events {
		if e.Type.IsCommand() {
			out = append(out, e)
		}
	}
	return out
}

func (c *Context) awaitCompletion(seq int64) (journal.Event, bool) {
	ev, ok := c.completions[seq]
	return ev, ok
}

func (c *Context) suspend() {
	runtime.Goexit()
}

type determinismPanic struct{ err error }

func Sleep(ctx *Context, d time.Duration) error {
	if ctx.canceled {
		return ErrCanceled
	}
	fireAt := ctx.now.Add(d)
	_ = fireAt // payload encoding added in Task 5; M0 uses empty payload for now
	ev, _ := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, nil)
	if _, ok := ctx.awaitCompletion(ev.Seq); !ok {
		ctx.suspend()
		return nil // unreachable
	}
	return nil
}
```

```go
// workflow/sleep.go — keep Sleep in context.go for Task 3, or move here if preferred.
```

```go
// internal/engine/run.go
package engine

import (
	"errors"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type Result struct {
	Suspended   bool
	Result      any
	Err         error
	NewCommands []journal.Event
	Stuck       bool
}

func Run(events []journal.Event, fn func(*workflow.Context) (any, error)) Result {
	return RunAt(events, time.Time{}, fn)
}

func RunAt(events []journal.Event, now time.Time, fn func(*workflow.Context) (any, error)) (out Result) {
	ctx := workflow.NewContext(events, now)
	done := make(chan struct{})
	var result any
	var fnErr error
	var stuck error

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if dp, ok := r.(interface{ Determinism() error }); ok {
					stuck = dp.Determinism()
				} else if e, ok := r.(error); ok && errors.Is(e, journal.ErrDeterminismViolation) {
					stuck = e
				} else {
					// re-panic unknown — but first check for our type
					type determ interface{ IsDeterminism() bool; error }
					if d, ok := r.(interface{ Error() string }); ok {
						// workflow.determinismPanic is unexported; use a public helper
						_ = d
					}
					if err, ok := workflow.AsDeterminismPanic(r); ok {
						stuck = err
					} else {
						fnErr = errors.New("workflow panic")
					}
				}
			}
			close(done)
		}()
		result, fnErr = fn(ctx)
	}()

	<-done

	out.NewCommands = ctx.NewCommands()
	if stuck != nil {
		out.Stuck = true
		out.Err = stuck
		return out
	}
	// If new commands were produced and function did not return normally via channel
	// semantics: Goexit leaves result zero and fnErr nil without completing.
	// Detect suspend: goroutine ended, no stuck, and either new unresolved commands
	// or we need an explicit suspended flag set by Context.
	if workflow.WasSuspended(ctx) {
		out.Suspended = true
		return out
	}
	out.Result = result
	out.Err = fnErr
	return out
}
```

Adjust `workflow` to export helpers:

```go
// additions to workflow/context.go
func (c *Context) markSuspended() { c.suspended = true }
func (c *Context) suspend() {
	c.suspended = true
	runtime.Goexit()
}
func WasSuspended(c *Context) bool { return c.suspended }

type determinismPanic struct{ err error }

func (d determinismPanic) Error() string          { return d.err.Error() }
func (d determinismPanic) Unwrap() error          { return d.err }
func (d determinismPanic) IsDeterminism() bool    { return true }

func AsDeterminismPanic(r any) (error, bool) {
	if d, ok := r.(determinismPanic); ok {
		return d.err, true
	}
	return nil, false
}

func raiseDeterminism(err error) {
	panic(determinismPanic{err: err})
}
```

Use `raiseDeterminism` inside `recordOrReplay` instead of raw panic.

Simplify `engine.Run` recover to only use `workflow.AsDeterminismPanic`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/engine/ ./workflow/ ./journal/ -count=1`
Expected: PASS

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-3-context-suspend`
Commit: `Add workflow.Context replay cursor and Goexit-based suspend.`
Merge before Task 4.

---

### Task 4: Determinism violation marks stuck

**Files:**
- Modify: `workflow/context.go` (already raises)
- Create: `internal/engine/determinism_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/engine/determinism_test.go
package engine_test

import (
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_DeterminismViolationStuck(t *testing.T) {
	events := []journal.Event{
		{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"},
		{Seq: 2, Type: journal.TypeActivityScheduled, Name: "Charge"},
		{Seq: 3, Type: journal.TypeActivityCompleted, RefSeq: 2, Payload: []byte(`"ok"`)},
	}
	res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
		// Code change: schedule different activity name → violation
		_, err := workflow.Execute[struct{}, string](ctx, "Refund", struct{}{})
		return nil, err
	})
	if !res.Stuck {
		t.Fatal("expected stuck")
	}
	if !errors.Is(res.Err, journal.ErrDeterminismViolation) {
		t.Fatalf("want determinism violation, got %v", res.Err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL（`Execute` 未定義）

- [ ] **Step 3: Implement Execute (sync, journal-only)**

```go
// workflow/execute.go
package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

func Execute[I, O any](ctx *Context, activityName string, in I) (O, error) {
	var zero O
	if ctx.canceled {
		// Execute is still allowed after cancel for compensation; do not return ErrCanceled here.
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return zero, err
	}
	ev, _ := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeActivityScheduled,
		Name: activityName,
	}, payload)

	comp, ok := ctx.awaitCompletion(ev.Seq)
	if !ok {
		ctx.suspend()
		return zero, nil
	}
	if comp.Type == journal.TypeActivityFailed {
		var msg string
		_ = json.Unmarshal(comp.Payload, &msg)
		return zero, fmt.Errorf("%s", msg)
	}
	var out O
	if err := json.Unmarshal(comp.Payload, &out); err != nil {
		return zero, err
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./... -count=1`
Expected: PASS

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-4-determinism-stuck`
Commit: `Detect determinism violations during replay and mark stuck.`

---

### Task 5: Codec, timer payload, and Sleep duration

**Files:**
- Create: `codec/codec.go`
- Create: `codec/json.go`
- Create: `codec/json_test.go`
- Modify: `workflow/sleep.go` (or `context.go`) to encode `fire_at`
- Create: `workflow/sleep_test.go`

- [ ] **Step 1: Write failing tests**

```go
// codec/json_test.go
package codec_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/codec"
)

func TestJSONRoundTrip(t *testing.T) {
	c := codec.JSON()
	type payload struct {
		FireAt time.Time `json:"fire_at"`
	}
	in := payload{FireAt: time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out payload
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !out.FireAt.Equal(in.FireAt) {
		t.Fatalf("got %v want %v", out.FireAt, in.FireAt)
	}
}
```

```go
// workflow/sleep_test.go via engine
func TestSleep_UsesFireAtPayload(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []journal.Event{{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"}}
	res := engine.RunAt(events, now, func(ctx *workflow.Context) (any, error) {
		return nil, workflow.Sleep(ctx, 7*24*time.Hour)
	})
	if !res.Suspended || len(res.NewCommands) != 1 {
		t.Fatalf("%+v", res)
	}
	var p struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(res.NewCommands[0].Payload, &p); err != nil {
		t.Fatal(err)
	}
	want := now.Add(7 * 24 * time.Hour)
	if !p.FireAt.Equal(want) {
		t.Fatalf("fire_at=%v want=%v", p.FireAt, want)
	}
}
```

- [ ] **Step 2–4:** Implement `codec.Codec`, update `Sleep` to marshal fire_at, pass tests.

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-5-codec-sleep`
Commit: `Add JSON codec and timer fire_at payload for Sleep.`

---

### Task 6: In-memory backend (M0 subset)

**Files:**
- Create: `backend/types.go`
- Create: `backend/errors.go`
- Create: `backend/memory/memory.go`
- Create: `backend/memory/memory_test.go`

M0 needs: create instance, append journal, load journal, store timers, fire timers into inbox, claim/complete activity conceptually. Keep the interface aligned with docs but implement only methods used by the engine/wftest in M0.

- [ ] **Step 1: Write failing test**

```go
// backend/memory/memory_test.go
package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMemory_CreateAndLoadJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "i1", Name: "WF", Queue: "default",
		Input: []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := b.LoadWorkflow(ctx, "i1")
	if err != nil {
		t.Fatal(err)
	}
	if st.NextSeq != 1 {
		t.Fatalf("next_seq=%d", st.NextSeq)
	}
	if len(st.Journal) != 1 || st.Journal[0].Type != journal.TypeWorkflowStarted {
		t.Fatalf("journal=%+v", st.Journal)
	}
}

func TestMemory_CommitAdvancement_CAS(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	_ = b.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"})
	st, _ := b.LoadWorkflow(ctx, "i1")
	// claim workflow task implicitly created
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1"})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %#v", err, tasks)
	}
	adv := backend.Advancement{
		InstanceID:  "i1",
		TaskID:      tasks[0].ID,
		ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{
			{Seq: st.NextSeq, Type: journal.TypeTimerCreated, Payload: []byte(`{"fire_at":"2026-01-08T00:00:00Z"}`)},
		},
		Timers: []backend.NewTimer{{Seq: st.NextSeq, FireAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)}},
	}
	if err := b.CommitAdvancement(ctx, adv); err != nil {
		t.Fatal(err)
	}
	// stale CAS fails
	adv.ExpectedSeq = st.NextSeq
	adv.TaskID = 999
	if err := b.CommitAdvancement(ctx, adv); err == nil {
		// may fail on missing task or CAS — either is fine; create fresh claim path in impl tests
	}
}
```

Refine tests during implementation so CAS conflict returns `backend.ErrConflict`.

- [ ] **Step 2–4:** Implement types + memory backend with mutex, instances, journal, inbox, tasks, timers. `Capabilities` returns unlimited. `Now()` is injectable clock for virtual time.

Expose:

```go
func (b *Backend) SetNow(t time.Time)
func (b *Backend) Now() time.Time
```

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-6-memory-backend`
Commit: `Add in-memory backend implementing M0 Backend contract.`

---

### Task 7: Worker loop for workflow + activity with memory backend

**Files:**
- Create: `registry/registry.go`
- Create: `worker/worker.go`
- Create: `worker/worker_test.go`
- Create: `client/client.go` (Start only for M0)

- [ ] **Step 1: Write failing durability test (acceptance core)**

```go
// worker/worker_test.go
package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestWorker_ReplayResumesFromPartialJournal(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	step1 := func(ctx context.Context, _ struct{}) (string, error) { return "a", nil }
	step2 := func(ctx context.Context, _ struct{}) (string, error) { return "b", nil }

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond})
	tasuki.RegisterActivity(w, step1, tasuki.WithName("step1"))
	tasuki.RegisterActivity(w, step2, tasuki.WithName("step2"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		a, err := workflow.Execute[struct{}, string](wctx, "step1", struct{}{})
		if err != nil {
			return "", err
		}
		b2, err := workflow.Execute[struct{}, string](wctx, "step2", struct{}{})
		if err != nil {
			return "", err
		}
		return a + b2, nil
	}, tasuki.WithName("WF"))

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{}, tasuki.WithID("id-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	defer w.Shutdown(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, "id-1")
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == "completed" {
			var out string
			if err := json.Unmarshal(info.Result, &out); err != nil {
				t.Fatal(err)
			}
			if out != "ab" {
				t.Fatalf("got %q", out)
			}
			_ = h
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}
```

Note: Registration API may need function-pointer based names; for M0, `Execute` takes activity name string as already designed in Task 4, and `RegisterActivity` binds name → func. Workflow registration similarly.

- [ ] **Step 2–4:** Implement registry, worker poll loop (workflow executor + activity executor), client Start/Get. On workflow suspend, commit new commands + activity tasks / timers. On activity complete, inbox + ensure workflow task.

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-7-worker-replay`
Commit: `Add worker and client that resume workflows from journal.`

---

### Task 8: Virtual clock and wftest prototype

**Files:**
- Create: `wftest/env.go`
- Create: `wftest/env_test.go`

- [ ] **Step 1: Write failing acceptance test (7-day sleep)**

```go
// wftest/env_test.go
package wftest_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/wftest"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_SevenDaySleepWithVirtualClock(t *testing.T) {
	env := wftest.New(t)
	wftest.RegisterActivity(env, func(ctx context.Context, _ struct{}) (string, error) {
		return "ping", nil
	}, wftest.WithName("ping"))

	res, err := wftest.Run(env, func(ctx *workflow.Context, _ struct{}) (string, error) {
		if _, err := workflow.Execute[struct{}, string](ctx, "ping", struct{}{}); err != nil {
			return "", err
		}
		if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
			return "", err
		}
		return "done", nil
	}, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res != "done" {
		t.Fatalf("got %q", res)
	}
}
```

- [ ] **Step 2–4:** Implement `wftest.New` wrapping memory backend + worker. When no runnable tasks remain, advance clock to earliest timer and fire due timers. Loop until workflow terminal or timeout.

- [ ] **Step 5: Commit and open PR**

Branch: `m0/task-8-wftest-virtual-clock`
Commit: `Add wftest package with virtual clock for durable timers.`

---

### Task 9: Example demo and README status update

**Files:**
- Create: `examples/m0-hello/main.go`
- Modify: `README.md`
- Modify: `docs/04-plan.md` (optional note that M0 implementation started)
- Modify: `.claude/tasks/todo.md`

- [ ] **Step 1: Add example that runs with memory backend**

```go
// examples/m0-hello/main.go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func HelloWorkflow(ctx *workflow.Context, name string) (string, error) {
	msg, err := workflow.Execute[string, string](ctx, "greet", name)
	if err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, time.Hour); err != nil {
		return "", err
	}
	return msg + " (after sleep)", nil
}

func Greet(ctx context.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func main() {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Now().UTC())
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: 10 * time.Millisecond})
	tasuki.RegisterWorkflow(w, HelloWorkflow, tasuki.WithName("hello"))
	tasuki.RegisterActivity(w, Greet, tasuki.WithName("greet"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "hello", "world", tasuki.WithID("demo-1"))
	if err != nil {
		panic(err)
	}

	// Drive virtual time for the demo
	for {
		info, _ := c.Get(ctx, "demo-1")
		if info.Status == "completed" {
			fmt.Println(string(info.Result))
			_ = h
			return
		}
		if n, _ := b.FireDueTimers(ctx, 10); n == 0 {
			// advance 1h if sleeping
			b.SetNow(b.Now().Add(time.Hour))
		}
		time.Sleep(5 * time.Millisecond)
	}
}
```

- [ ] **Step 2: Update README status** from「設計フェーズ」to「M0 実装中 / M0 完了」as appropriate. Link to plan.

- [ ] **Step 3: Run full suite**

Run: `go test ./... -race -count=1`
Expected: PASS

Run: `go run ./examples/m0-hello/`
Expected: prints greeting

- [ ] **Step 4: Commit and open PR**

Branch: `m0/task-9-example-readme`
Commit: `Add M0 hello example and mark implementation status in README.`

---

## M0 acceptance checklist

| Criterion | Covered by |
|---|---|
| Multi-step workflow resumes from partial journal | Task 7 test |
| Determinism violation → stuck | Task 4 test |
| defer / recover do not break suspend | Task 3 tests |
| 7-day sleep with virtual clock | Task 8 test |

---

## Self-review

1. **Spec coverage:** M0 scope in 04-plan is covered by Tasks 1–9. Full Client API, PostgreSQL, signals, children are out of scope (M1+).
2. **Placeholders:** None intentional; Task 6–7 leave room to refine method signatures to match `backend.Backend` in 02-architecture while keeping M0-only methods implemented.
3. **Type consistency:** `workflow.Execute` uses activity name string in M0 (registration maps name → func). Typed `Execute(ctx, fn, in)` sugar can wrap this in M1/M2 once registry reflection is stable.

---

## Execution notes

- Work on feature branches; **one PR per task**; merge to `main` before the next task.
- Follow TDD: red → green → refactor for each task.
- Do not use subagents.
- After Task 9, run `go test ./... -race` and confirm all four acceptance criteria.
