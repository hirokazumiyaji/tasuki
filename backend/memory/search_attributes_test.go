package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func TestMemory_SearchAttributes(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sa-1", Name: "WF", Queue: "default",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "new"},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, "sa-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["tenant"] != "acme" || inst.SearchAttributes["phase"] != "new" {
		t.Fatalf("get: %#v", inst.SearchAttributes)
	}

	list, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme"},
	})
	if err != nil || len(list) != 1 || list[0].ID != "sa-1" {
		t.Fatalf("list: %+v err=%v", list, err)
	}
	miss, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "other"},
	})
	if err != nil || len(miss) != 0 {
		t.Fatalf("want empty, got %+v", miss)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %+v err=%v", tasks, err)
	}
	st, err := b.LoadWorkflow(ctx, "sa-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: "sa-1", TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq:     st.NextSeq,
			Type:    journal.TypeSearchAttributesUpdated,
			Payload: []byte(`{"tenant":"acme","phase":"shipped"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err = b.GetInstance(ctx, "sa-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["phase"] != "shipped" {
		t.Fatalf("after upsert: %#v", inst.SearchAttributes)
	}
	list, err = b.ListInstances(ctx, backend.InstanceFilter{
		Status:           "running",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "shipped"},
	})
	if err != nil || len(list) != 1 {
		t.Fatalf("list and: %+v err=%v", list, err)
	}
}
