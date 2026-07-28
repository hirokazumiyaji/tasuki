package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMemory_Memo(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "memo-1", Name: "WF", Queue: "default",
		Memo: map[string]string{"note": "vip"},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, "memo-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip" {
		t.Fatalf("get: %#v", inst.Memo)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %+v err=%v", tasks, err)
	}
	st, err := b.LoadWorkflow(ctx, "memo-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "memo-1", TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq:     st.NextSeq,
			Type:    journal.TypeMemoUpdated,
			Payload: []byte(`{"note":"vip-shipped"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err = b.GetInstance(ctx, "memo-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip-shipped" {
		t.Fatalf("after upsert: %#v", inst.Memo)
	}
}
