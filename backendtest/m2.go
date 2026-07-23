package backendtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// RunM2 executes M2 conformance cases (signals, children, cancel compensation).
func RunM2(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("SignalCommitRaceI1", func(t *testing.T) { testSignalCommitRace(t, newBackend) })
	t.Run("ChildCompletionNotifiesParent", func(t *testing.T) { testChildNotify(t, newBackend) })
	t.Run("CancelCompensation", func(t *testing.T) { testCancelCompensation(t, newBackend) })
}

func testSignalCommitRace(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	const rounds = 40
	for r := 0; r < rounds; r++ {
		b := newBackend(t)
		setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		id := fmt.Sprintf("sig-race-%d", r)
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
		tasks, err := b.ClaimTasks(ctx, claimWF())
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim: %v %#v", err, tasks)
		}
		st, _ := b.LoadWorkflow(ctx, id)
		fireAt := st.Now.Add(time.Hour)
		if _, ok := b.(ClockSetter); !ok {
			fireAt = time.Now().Add(time.Hour)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var sendErr, commitErr error
		go func() {
			defer wg.Done()
			sendErr = b.SendToInbox(ctx, id, journal.Event{
				Type:    journal.TypeSignalReceived,
				Name:    "approve",
				Payload: []byte(`{}`),
			})
		}()
		go func() {
			defer wg.Done()
			commitErr = b.CommitAdvancement(ctx, backend.Advancement{
				InstanceID:  id,
				TaskID:      tasks[0].ID,
				ExpectedSeq: st.NextSeq,
				NewEvents:   []journal.Event{{Seq: st.NextSeq, Type: journal.TypeTimerCreated}},
				Timers:      []backend.NewTimer{{Seq: st.NextSeq, FireAt: fireAt}},
			})
		}()
		wg.Wait()
		if sendErr != nil {
			t.Fatalf("round %d: SendToInbox: %v", r, sendErr)
		}
		if commitErr != nil {
			t.Fatalf("round %d: CommitAdvancement: %v", r, commitErr)
		}
		assertI1(t, b, id)
	}
}

func testChildNotify(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	parentID := "parent-notify"
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: parentID, Name: "Parent", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim parent: %v %#v", err, tasks)
	}
	st, _ := b.LoadWorkflow(ctx, parentID)
	childSeq := st.NextSeq
	childID := parentID + ":child"
	payload, _ := json.Marshal(map[string]any{"child_id": childID, "name": "Child", "input": json.RawMessage(`null`)})
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  parentID,
		TaskID:      tasks[0].ID,
		ExpectedSeq: childSeq,
		NewEvents: []journal.Event{{
			Seq: childSeq, Type: journal.TypeChildScheduled, Name: "Child", Payload: payload,
		}},
		Children: []backend.NewInstance{{
			ID: childID, Name: "Child", Queue: "default",
			ParentID: parentID, ParentSeq: childSeq,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	ctasks, err := b.ClaimTasks(ctx, claimWF())
	if err != nil || len(ctasks) != 1 || ctasks[0].InstanceID != childID {
		t.Fatalf("claim child: %v %#v", err, ctasks)
	}
	cst, _ := b.LoadWorkflow(ctx, childID)
	result := []byte(`"ok"`)
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID:  childID,
		TaskID:      ctasks[0].ID,
		ExpectedSeq: cst.NextSeq,
		NewEvents: []journal.Event{{
			Seq: cst.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: result,
		}},
		Terminal: &backend.TerminalUpdate{Status: "completed", Result: result},
		ParentNotify: &journal.Event{
			Type: journal.TypeChildCompleted, RefSeq: childSeq, Payload: result,
		},
	}); err != nil {
		t.Fatal(err)
	}

	pst, err := b.LoadWorkflow(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range pst.Inbox {
		if item.Event.Type == journal.TypeChildCompleted && item.Event.RefSeq == childSeq {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("parent inbox missing child_completed: %+v", pst.Inbox)
	}
	assertI1(t, b, parentID)
}

func testCancelCompensation(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	comp := false
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond, LeaseDuration: time.Minute})
	tasuki.RegisterActivity(w, func(ctx context.Context, _ struct{}) (string, error) {
		comp = true
		return "refunded", nil
	}, tasuki.WithName("refund"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (string, error) {
		err := workflow.Sleep(wctx, time.Hour)
		if errors.Is(err, workflow.ErrCanceled) {
			if _, err := workflow.Execute[struct{}, string](wctx, "refund", struct{}{}); err != nil {
				return "", err
			}
			return "", workflow.ErrCanceled
		}
		return "done", err
	}, tasuki.WithName("cancelWF"))
	w.Start(ctx)
	defer w.Shutdown(ctx)

	c := tasuki.NewClient(b)
	id := "cancel-comp-1"
	if _, err := tasuki.Start(ctx, c, "cancelWF", struct{}{}, tasuki.WithID(id)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := b.LoadWorkflow(ctx, id)
		if err == nil && len(st.Journal) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		info, err := c.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status == "canceled" {
			if !comp {
				t.Fatal("compensation activity should have run")
			}
			return
		}
		if info.Status == "failed" || info.Status == "completed" {
			t.Fatalf("status=%s failure=%s", info.Status, string(info.Failure))
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for canceled")
}
