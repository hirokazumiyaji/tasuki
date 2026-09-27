package tasuki_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// fenceFailBackend injects transient instance-read failures while fail is
// set, then delegates to memory.
type fenceFailBackend struct {
	*memory.Backend
	fail atomic.Bool
	errs atomic.Int64
}

func (f *fenceFailBackend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	if f.fail.Load() {
		f.errs.Add(1)
		return nil, errors.New("injected transient instance read failure")
	}
	return f.Backend.GetInstance(ctx, id)
}

func claimableActivity(t *testing.T, ctx context.Context, b backend.Backend) int64 {
	t.Helper()
	m, err := b.CountClaimableTasks(ctx, "activity", []string{"default"})
	if err != nil {
		t.Fatal(err)
	}
	return m["default"]
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The activity pre-execution status fence must fail CLOSED: while the
// owning instance's status cannot be read, the worker must not invoke user
// code (the lease expires and the task redelivers). Failing open would run
// activities of possibly-terminated instances on every transient read.
//
// The activity task is staged directly through CommitAdvancement so the
// fence window is fully controlled: no workflow tick can consume or
// schedule anything mid-test.
func TestActivityFence_FailsClosedOnStatusReadError(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fb := &fenceFailBackend{Backend: mem}

	const id = "fence-1"
	if err := fb.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	wfTasks, err := fb.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1,
		Lease: time.Minute, WorkerID: "stage",
	})
	if err != nil || len(wfTasks) != 1 {
		t.Fatalf("stage workflow claim: %v n=%d", err, len(wfTasks))
	}
	st, err := fb.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := fb.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  id,
		TaskID:      wfTasks[0].ID,
		ExpectedSeq: st.NextSeq,
		ActivityTasks: []backend.NewTask{{
			Kind: "activity", Queue: "default", InstanceID: id,
			Name: "probe", Seq: st.NextSeq, MaxAttempts: 1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "activity task claimable", 5*time.Second, func() bool {
		return claimableActivity(t, ctx, fb) > 0
	})

	var invocations atomic.Int64
	w := tasuki.NewWorker(fb, tasuki.WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
	})
	tasuki.RegisterActivity(w, func(context.Context, struct{}) (string, error) {
		invocations.Add(1)
		return "ok", nil
	}, tasuki.WithName("probe"))
	// Break status reads before the worker's first poll: every activity
	// claim from here on runs into the fence error.
	fb.fail.Store(true)
	w.Start(ctx)
	defer w.Shutdown(ctx)

	// Wait until the worker holds the activity lease behind at least one
	// failed fence check.
	waitFor(t, "activity lease held behind fence errors", 5*time.Second, func() bool {
		return claimableActivity(t, ctx, fb) == 0 && fb.errs.Load() >= 1
	})
	// Several poll cycles pass with the lease held: fail-open code would
	// have invoked the activity on the first claim.
	time.Sleep(300 * time.Millisecond)
	if n := invocations.Load(); n != 0 {
		t.Fatalf("activity invoked %d times while instance status unreadable: fence must fail closed", n)
	}
	// Heal reads and expire the outstanding lease so the task redelivers;
	// the activity must then run exactly once and complete.
	fb.fail.Store(false)
	mem.SetNow(time.Now().UTC())
	waitFor(t, "activity completion", 5*time.Second, func() bool {
		return invocations.Load() == 1 && claimableActivity(t, ctx, fb) == 0
	})
	time.Sleep(100 * time.Millisecond)
	if n := invocations.Load(); n != 1 {
		t.Fatalf("activity invoked %d times, want exactly 1", n)
	}
}
