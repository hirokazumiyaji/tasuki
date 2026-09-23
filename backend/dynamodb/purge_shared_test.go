package dynamodb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// purgeTaskKeyForTargets attributes scanned task rows to the purge victims
// of one PurgeInstances call: only rows carrying a target instance_id plus a
// deletable task_pk are selected; live-instance rows and malformed rows are
// skipped.
func TestPurgeTaskKeyForTargets_Attribution(t *testing.T) {
	targets := purgeTaskTargets([]string{"A", "B"})
	row := func(inst, pk string) map[string]types.AttributeValue {
		m := map[string]types.AttributeValue{}
		if inst != "" {
			m["instance_id"] = avS(inst)
		}
		if pk != "" {
			m["task_pk"] = avS(pk)
		}
		return m
	}
	cases := []struct {
		name string
		m    map[string]types.AttributeValue
		want string // want task_pk, "" = skip
	}{
		{"target A", row("A", "WF#A"), "WF#A"},
		{"target B", row("B", "ACT#7"), "ACT#7"},
		{"live instance untouched", row("C", "WF#C"), ""},
		{"missing instance_id skipped", row("", "WF#X"), ""},
		{"missing task_pk skipped", row("A", ""), ""},
	}
	for _, tc := range cases {
		pk, ok := purgeTaskKeyForTargets(tc.m, targets)
		if tc.want == "" {
			if ok {
				t.Errorf("%s: selected, want skip", tc.name)
			}
			continue
		}
		if !ok || fromS(pk) != tc.want {
			t.Errorf("%s: got (%v, %v), want pk %q", tc.name, pk, ok, tc.want)
		}
	}
	if got := len(purgeTaskTargets(nil)); got != 0 {
		t.Fatalf("empty victim list: targets = %d, want 0", got)
	}
}

// deleteTasksForInstancesByScan covers every victim with exactly ONE fleet
// Scan: a two-victim batch must cost one Scan (not one per victim) and
// delete only target rows.
func TestDeleteTasksForInstancesByScan_SharesOneScan(t *testing.T) {
	f := &fakeDynamo{
		scanItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#A"), "instance_id": avS("A")},
			{"task_pk": avS("ACT#1"), "instance_id": avS("A")},
			{"task_pk": avS("WF#B"), "instance_id": avS("B")},
			{"task_pk": avS("WF#LIVE"), "instance_id": avS("LIVE")},
		},
	}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstancesByScan(context.Background(), []string{"A", "B"}); err != nil {
		t.Fatalf("shared scan: %v", err)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 1 {
		t.Fatalf("Scan calls = %d, want 1 (one shared scan per batch, not per victim)", got)
	}
	want := map[string]bool{"WF#A": true, "ACT#1": true, "WF#B": true}
	if len(f.deleted) != len(want) {
		t.Fatalf("deleted = %v, want exactly %v", f.deleted, want)
	}
	for _, pk := range f.deleted {
		if !want[pk] {
			t.Fatalf("deleted = %v, want exactly %v (live rows must survive)", f.deleted, want)
		}
	}
}

// An empty victim list must not touch the table at all.
func TestDeleteTasksForInstancesFull_EmptyIsNoop(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstancesFull(context.Background(), nil); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 0 {
		t.Fatalf("Scan calls = %d, want 0", got)
	}
	if got := atomic.LoadInt64(&f.queryCalls); got != 0 {
		t.Fatalf("Query calls = %d, want 0", got)
	}
}

// The per-instance GSI sweep is retained for every victim (cheap,
// instance-keyed) while the fleet Scan stays shared: two victims cost two
// Queries but one Scan.
func TestDeleteTasksForInstancesFull_GSISweepPerInstanceSharedScan(t *testing.T) {
	f := &fakeDynamo{
		queryItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#A"), "instance_id": avS("A")},
		},
		scanItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#B"), "instance_id": avS("B")},
		},
	}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstancesFull(context.Background(), []string{"A", "B"}); err != nil {
		t.Fatalf("full: %v", err)
	}
	if got := atomic.LoadInt64(&f.queryCalls); got != 2 {
		t.Fatalf("Query calls = %d, want 2 (per-instance GSI sweep retained)", got)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 1 {
		t.Fatalf("Scan calls = %d, want 1 (shared across the batch)", got)
	}
}

// Without a queryable index the GSI probe fails table-wide: probing stops
// after the first victim instead of failing identically per victim, and the
// shared Scan performs the whole cleanup.
func TestDeleteTasksForInstancesFull_GSIMissingStopsProbing(t *testing.T) {
	f := &fakeDynamo{
		queryErr: errors.New("ValidationException: The table does not have the specified index: instance_gsi"),
		scanItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#A"), "instance_id": avS("A")},
			{"task_pk": avS("WF#B"), "instance_id": avS("B")},
		},
	}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstancesFull(context.Background(), []string{"A", "B"}); err != nil {
		t.Fatalf("full without index: %v", err)
	}
	if got := atomic.LoadInt64(&f.queryCalls); got != 1 {
		t.Fatalf("Query calls = %d, want 1 (stop probing after the table-wide miss)", got)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 1 {
		t.Fatalf("Scan calls = %d, want 1 (shared scan is the whole cleanup)", got)
	}
	if len(f.deleted) != 2 {
		t.Fatalf("deleted = %v, want both victim rows via the shared scan", f.deleted)
	}
}
