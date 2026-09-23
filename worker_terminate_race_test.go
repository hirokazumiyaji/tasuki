package tasuki_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// terminateRaceBackend commits a real termination of the target instance
// inside its first GetInstance: the read observes "running" and returns
// that stale snapshot, while the termination commits before the caller acts
// on it. This deterministically reproduces the GetInstance(running) ->
// TerminateInstance -> invokeActivity race without timing dependence: with
// only the entry check, the activity runs once after termination; with the
// pre-invoke re-check, the second read observes the committed termination
// and the task is dropped.
type terminateRaceBackend struct {
	*memory.Backend
	id    string
	reads atomic.Int64
}

func (f *terminateRaceBackend) GetInstance(ctx context.Context, id string) (*backend.Instance, error) {
	inst, err := f.Backend.GetInstance(ctx, id)
	if err == nil && id == f.id && f.reads.Add(1) == 1 {
		// Termination commits after the first fence check observed
		// "running" but before the handler invokes user code.
		_ = f.Backend.TerminateInstance(ctx, id)
	}
	return inst, err
}

// The termination right must stay fenced through activity invocation: a
// TerminateInstance that commits after the entry status check but before
// user code runs must still drop the task instead of invoking the activity.
// The activity task is staged directly through CommitAdvancement so the
// race window is fully controlled: no workflow tick can consume or
// schedule anything mid-test.
func TestActivityFence_TerminateBetweenCheckAndInvoke(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	mem.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fb := &terminateRaceBackend{Backend: mem, id: "term-race-1"}

	const id = "term-race-1"
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
	w.Start(ctx)
	defer w.Shutdown(ctx)

	// The injected termination commits during handling (on the second
	// status read); wait until it is visible, then allow the handler to
	// settle. The fence cleanup deletes nothing further (TerminateInstance
	// already swept the task), so the task must simply be gone.
	waitFor(t, "injected termination", 5*time.Second, func() bool {
		inst, err := mem.GetInstance(ctx, id)
		return err == nil && inst.Status == "terminated"
	})
	time.Sleep(300 * time.Millisecond)
	if n := invocations.Load(); n != 0 {
		t.Fatalf("activity invoked %d times after termination committed before invocation: termination right must stay fenced through invokeActivity", n)
	}
	// Advance past the activity lease: a dropped task must not redeliver
	// and run late either.
	mem.SetNow(time.Now().UTC())
	time.Sleep(300 * time.Millisecond)
	if n := invocations.Load(); n != 0 {
		t.Fatalf("activity invoked %d times after lease expiry, want 0", n)
	}
	if n := claimableActivity(t, ctx, fb); n != 0 {
		t.Fatalf("claimable activity tasks = %d, want 0 (terminated task must not redeliver)", n)
	}
}
