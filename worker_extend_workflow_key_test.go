package tasuki_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// extendKeySpy records the task identity carried by each ExtendLease call.
// Before the workflow-key fix, renewal passed only the numeric task ID, so on
// DynamoDB/Firestore (workflow tasks under WF#<instanceID>, not ACT#<id>)
// workflow-task renewals missed and returned ErrNotFound.
type extendKeySpy struct {
	backend.Backend
	mu      sync.Mutex
	extends []backend.Task
}

func (s *extendKeySpy) ExtendLease(ctx context.Context, t backend.Task, d time.Duration) error {
	s.mu.Lock()
	s.extends = append(s.extends, t)
	s.mu.Unlock()
	return s.Backend.ExtendLease(ctx, t, d)
}

func (s *extendKeySpy) workflowExtends() []backend.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []backend.Task
	for _, t := range s.extends {
		if t.Kind == "workflow" {
			out = append(out, t)
		}
	}
	return out
}

// TestWorkflow_RenewalCarriesWorkflowKey is a regression test for the
// workflow-lease renewal finding: ExtendLease must receive the claimed
// workflow task (kind + instance ID) so DynamoDB/Firestore can address the
// WF#<instanceID> key instead of the missing ACT#<id> key. A slow local
// activity spans several lease periods; without the fix no renewal carries
// workflow identity and long replays lose their lease to a peer.
func TestWorkflow_RenewalCarriesWorkflowKey(t *testing.T) {
	ctx := context.Background()
	spy := &extendKeySpy{Backend: memory.New()}
	spy.Backend.(*memory.Backend).SetNow(time.Now().UTC())

	slowLocal := func(actCtx context.Context, n int) (int, error) {
		select {
		case <-actCtx.Done():
			return 0, actCtx.Err()
		case <-time.After(600 * time.Millisecond):
			return n * 2, nil
		}
	}
	slowWF := func(wctx *workflow.Context, n int) (int, error) {
		return workflow.ExecuteLocal[int, int](wctx, "slowLocal", n)
	}

	c := tasuki.NewClient(spy)
	sh, err := tasuki.Start(ctx, c, "slowWF", 7, tasuki.WithID("extend-wf-key-1"))
	if err != nil {
		t.Fatal(err)
	}

	w := tasuki.NewWorker(spy, tasuki.WorkerOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: 150 * time.Millisecond,
		ClaimLimit:    1,
		WorkerID:      "w1",
	})
	tasuki.RegisterActivity(w, slowLocal, tasuki.WithName("slowLocal"))
	tasuki.RegisterWorkflow(w, slowWF, tasuki.WithName("slowWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := c.Get(ctx, sh.ID())
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == "completed" {
			if got := string(info.Result); got != "14" {
				t.Fatalf("result = %s, want 14", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow did not complete (status %s); extends: %+v", info.Status, spy.extends)
		}
		time.Sleep(20 * time.Millisecond)
	}

	wfExtends := spy.workflowExtends()
	if len(wfExtends) == 0 {
		t.Fatalf("no ExtendLease carried workflow identity (all extends: %+v)", spy.extends)
	}
	for _, got := range wfExtends {
		if got.InstanceID != "extend-wf-key-1" {
			t.Fatalf("workflow ExtendLease instance = %q, want extend-wf-key-1", got.InstanceID)
		}
		if got.ID == 0 {
			t.Fatal("workflow ExtendLease carried a zero task ID")
		}
	}
}
