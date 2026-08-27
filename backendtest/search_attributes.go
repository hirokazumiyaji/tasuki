package backendtest

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func testSearchAttributes(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("sa-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "WF", Queue: "default",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "new"},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["tenant"] != "acme" || inst.SearchAttributes["phase"] != "new" {
		t.Fatalf("get attrs: %#v", inst.SearchAttributes)
	}

	list, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme"},
	})
	if err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("list: %+v err=%v", list, err)
	}
	miss, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "other"},
	})
	if err != nil || len(miss) != 0 {
		t.Fatalf("want empty, got %+v err=%v", miss, err)
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
	if st.Instance.SearchAttributes["tenant"] != "acme" {
		t.Fatalf("load attrs: %#v", st.Instance.SearchAttributes)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq:     st.NextSeq,
			Type:    journal.TypeSearchAttributesUpdated,
			Payload: []byte(`{"tenant":"acme","phase":"shipped"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err = b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["phase"] != "shipped" || inst.SearchAttributes["tenant"] != "acme" {
		t.Fatalf("after upsert: %#v", inst.SearchAttributes)
	}
	andList, err := b.ListInstances(ctx, backend.InstanceFilter{
		Status:           "running",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "shipped"},
	})
	if err != nil || len(andList) != 1 || andList[0].ID != id {
		t.Fatalf("AND list: %+v err=%v", andList, err)
	}
}
