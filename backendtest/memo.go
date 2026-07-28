package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func testMemo(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := "memo-" + t.Name()
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "WF", Queue: "default",
		Memo: map[string]string{"note": "vip"},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip" {
		t.Fatalf("get memo: %#v", inst.Memo)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %+v err=%v", tasks, err)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Instance.Memo["note"] != "vip" {
		t.Fatalf("load memo: %#v", st.Instance.Memo)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq:     st.NextSeq,
			Type:    journal.TypeMemoUpdated,
			Payload: []byte(`{"note":"vip-shipped"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err = b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Memo["note"] != "vip-shipped" {
		t.Fatalf("after upsert: %#v", inst.Memo)
	}
}
