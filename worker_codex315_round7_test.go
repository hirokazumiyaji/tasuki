package tasuki

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// countTaskReleaseBackend counts ReleaseLease calls while delegating to
// the wrapped backend.
type countTaskReleaseBackend struct {
	backend.Backend
	releases atomic.Int32
}

func (b *countTaskReleaseBackend) ReleaseLease(ctx context.Context, t backend.Task) error {
	b.releases.Add(1)
	return b.Backend.ReleaseLease(ctx, t)
}

// TestWorkflow_StalePendingCommitSkippedAfterRelease is a regression test
// for the pending-commit fencing finding: a shutdown-timeout
// releaseInFlight releases a finished pending turn before its tick
// flushes; a peer reclaims the released task; the old tick's flush must
// then skip the stale advancement instead of committing (and deleting)
// the peer's active task after duplicate execution, since backends
// validate the advancement by task ID alone.
//
// Interleaving (deterministic, no gates): the turn is driven directly to
// a pending advancement, then the real releaseInFlight runs (the
// shutdown-timeout release), a peer reclaims, and only then does the old
// tick flush. With the fix the flush checks in-flight ownership and
// skips the entry (returning it failed without touching the store): the
// instance is still running and the peer's lease is intact. Without the
// fix the flush commits the stale advancement on the memory backend and
// the instance completes on top of the peer's active task.
func TestWorkflow_StalePendingCommitSkippedAfterRelease(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Now().UTC())
	store := &countTaskReleaseBackend{Backend: mem}
	w := NewWorker(store, WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
	})
	RegisterWorkflow(w, func(_ *workflow.Context, _ struct{}) (string, error) {
		return "ok", nil
	}, WithName("WF"))
	c := NewClient(store)
	h, err := Start(ctx, c, "WF", struct{}{}, WithID("stale-flush-1"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %#v", err, claimed)
	}
	task := claimed[0]
	w.track(task)
	p, herr := w.handleWorkflow(ctx, task, func() {})
	if herr != nil {
		t.Fatalf("handleWorkflow = %v, want a pending advancement", herr)
	}
	if p == nil {
		t.Fatal("handleWorkflow returned no pending advancement")
	}
	p.task = task

	// Shutdown timeout: the finished pending turn is still tracked, so
	// releaseInFlight releases it before the old tick flushes.
	w.releaseInFlight(context.Background())
	if n := store.releases.Load(); n != 1 {
		t.Fatalf("ReleaseLease calls=%d, want 1", n)
	}

	// A peer reclaims the released task while the old tick still holds
	// its pending advancement.
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(peer) != 1 {
		t.Fatalf("peer claimed %d tasks, want 1", len(peer))
	}

	// The old tick flushes after the release + reclaim.
	failed := w.flushWorkflowCommits(ctx, []pendingWorkflowCommit{*p})
	if len(failed) != 1 {
		t.Fatalf("flush failed=%d entries, want 1 (skipped stale commit)", len(failed))
	}

	// The stale advancement must not have committed on top of the peer's
	// active task: the instance is still running (a stale flush would
	// have completed it) and the peer's lease is intact.
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusRunning {
		t.Fatalf("status=%s, want running (stale pending commit must be skipped after the release)", info.Status)
	}
	third, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10,
		Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third worker claimed %d tasks, want 0 (peer lease must stay intact)", len(third))
	}
	if n := store.releases.Load(); n != 1 {
		t.Fatalf("ReleaseLease calls=%d, want exactly 1 (skipped flush must not release again)", n)
	}
}

// TestWorker_StaleRenewalStopsQuietly is a regression test for the
// fenced-renewal finding at the worker layer: once the backend fence
// rejects a renewal (ErrNotFound — the lease moved on to a peer), the
// renewal loop must stop quietly instead of warning on every tick and
// retrying a renewal the backend will keep rejecting.
func TestWorker_StaleRenewalStopsQuietly(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	var logBuf bytes.Buffer
	w := NewWorker(mem, WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: 100 * time.Millisecond, // 50ms renewal ticks
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %#v", err, stale)
	}

	// The lease lapses and a peer reclaims the task; the stale holder's
	// renewal is now fenced out at the backend.
	mem.SetNow(t0.Add(2 * time.Minute))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		w.extendLeaseLoop(ctx, stale[0], done, nil)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		close(done)
		t.Fatal("stale renewal loop did not stop after the backend fence rejected it")
	}
	close(done)
	if logs := logBuf.String(); strings.Contains(logs, "store operation failed") {
		t.Fatalf("stale renewal logged as a failure; want quiet already-moved-on handling:\n%s", logs)
	}
}

// TestAcceptLocalResultUsesCompletionTime is a regression test for the
// completion-time finding: on-time/late arbitration must compare the
// production-time completion instant against the call deadline, not
// sample ctx state at consumption. A result produced just before the
// deadline whose timer fires first is on time; a result produced after
// the deadline is late even if the timer has not fired yet.
func TestAcceptLocalResultUsesCompletionTime(t *testing.T) {
	const timeout = 50 * time.Millisecond

	// A result produced just before the deadline but consumed after the
	// timer fired: on time, preserved (sampling ctx at consumption would
	// mark it late).
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	dl, ok := expired.Deadline()
	if !ok {
		t.Fatal("test setup: acceptance context should have a deadline")
	}
	justBefore := dl.Add(-time.Millisecond)
	time.Sleep(10 * time.Millisecond) // let the deadline pass deterministically
	if expired.Err() == nil {
		t.Fatal("test setup: acceptance context should be expired")
	}
	if out, err := acceptLocalResult("fast", callResult{out: []byte("ok"), completed: justBefore}, expired, context.Background(), timeout); err != nil || string(out) != "ok" {
		t.Fatalf("pre-deadline success discarded (out=%q err=%v), want preserved", out, err)
	}

	// A result produced exactly at the deadline is on time.
	if out, err := acceptLocalResult("edge", callResult{out: []byte("ok"), completed: dl}, expired, context.Background(), timeout); err != nil || string(out) != "ok" {
		t.Fatalf("at-deadline success discarded (out=%q err=%v), want preserved", out, err)
	}

	// A result produced after the deadline is late even if the timer has
	// not fired yet: judge by the stamp, not by consumption-time ctx
	// state. The acceptance context here is still live (hour-long
	// deadline), so consumption-time sampling would wrongly accept it.
	live, liveCancel := context.WithTimeout(context.Background(), time.Hour)
	defer liveCancel()
	liveDL, ok := live.Deadline()
	if !ok {
		t.Fatal("test setup: live context should have a deadline")
	}
	late := liveDL.Add(time.Second) // fabricated post-deadline production instant
	if _, err := acceptLocalResult("slow", callResult{out: []byte("late-ok"), completed: late}, live, context.Background(), timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-deadline success accepted (err=%v), want deadline exceeded", err)
	}

	// The faithful timer-not-fired race: the deadline has passed but the
	// runtime timer has not run, so the acceptance context is still
	// live while a genuinely-produced (hence pre-now, post-deadline)
	// result arrives. It must still be a timeout.
	unfired := pastDeadlineLiveContext(time.Second)
	if _, err := acceptLocalResult("race", callResult{out: []byte("late-ok"), completed: time.Now()}, unfired, context.Background(), timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timer-not-fired late success accepted (err=%v), want deadline exceeded", err)
	}

	// Missing timestamp falls back to consumption-time state (live
	// passes, expired times out) rather than misjudging.
	if out, err := acceptLocalResult("ok", callResult{out: []byte("v")}, live, context.Background(), timeout); err != nil || string(out) != "v" {
		t.Fatalf("live success without stamp: out=%q err=%v", out, err)
	}
	if _, err := acceptLocalResult("late", callResult{out: []byte("v")}, expired, context.Background(), timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired acceptance without stamp (err=%v), want deadline exceeded", err)
	}
}

// pastDeadlineLiveContext simulates the timer-not-fired race: a context
// whose deadline has passed but whose Done channel has not fired (still
// live to ctx.Err), so arbitration must come from the completion stamp.
type pastDeadlineLiveContext time.Duration

func (c pastDeadlineLiveContext) Deadline() (time.Time, bool) {
	return time.Now().Add(-time.Duration(c)), true
}

func (c pastDeadlineLiveContext) Done() <-chan struct{} { return nil }

func (c pastDeadlineLiveContext) Err() error { return nil }

func (c pastDeadlineLiveContext) Value(key any) any { return nil }

// TestLocalCompletedOnTimeFallback pins the no-deadline fallback: a
// context without a deadline arbitrates on consumption-time state.
func TestLocalCompletedOnTimeFallback(t *testing.T) {
	live, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()
	if !localCompletedOnTime(time.Now(), live) {
		t.Fatal("live context without deadline should arbitrate on time")
	}
	dead, stop := context.WithCancel(context.Background())
	stop()
	if localCompletedOnTime(time.Now(), dead) {
		t.Fatal("canceled context without deadline should arbitrate late")
	}
}
