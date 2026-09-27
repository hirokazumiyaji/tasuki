package dynamodb

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// fakeRecoverStore stubs the DynamoDB subset used by orphan recovery. Scan
// serves the ordered ids with DynamoDB-like pagination: ExclusiveStartKey is
// exclusive, and LastEvaluatedKey is the last item of a non-final page.
// Every running instance reports one inbox event and no workflow task, so
// each check recovers one orphan.
type fakeRecoverStore struct {
	ids        []string
	pageSize   int // 0 = single page
	scanStarts []string
	puts       int
	// instanceStatus overrides the live status returned by the pre-put
	// re-check (default "running"). Set to "terminated" to model a terminal
	// commit landing between the scan and the put.
	instanceStatus string
}

func (f *fakeRecoverStore) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	var startID string
	if in.ExclusiveStartKey != nil {
		startID = fromS(in.ExclusiveStartKey["id"])
	}
	f.scanStarts = append(f.scanStarts, startID)
	idx := 0
	if startID != "" {
		found := false
		for i, id := range f.ids {
			if id == startID {
				idx = i + 1
				found = true
				break
			}
		}
		if !found {
			return &dynamodb.ScanOutput{}, nil
		}
	}
	rest := f.ids[idx:]
	n := len(rest)
	if f.pageSize > 0 && n > f.pageSize {
		n = f.pageSize
	}
	items := make([]map[string]types.AttributeValue, 0, n)
	for _, id := range rest[:n] {
		items = append(items, map[string]types.AttributeValue{
			"id": avS(id), "status": avS("running"), "queue": avS("default"),
		})
	}
	out := &dynamodb.ScanOutput{Items: items}
	if idx+n < len(f.ids) {
		out.LastEvaluatedKey = map[string]types.AttributeValue{"id": avS(f.ids[idx+n-1])}
	}
	return out, nil
}

func (f *fakeRecoverStore) Query(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
		{"instance_id": avS("x")},
	}}, nil
}

func (f *fakeRecoverStore) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	// recoverPutIfRunning re-reads the instance row immediately before the
	// Put (round-17 P2 on #291): serve a live running instance for the
	// wf_instances table, and no workflow task for wf_tasks (every checked
	// instance is an orphan).
	if in.TableName != nil && len(*in.TableName) >= len("wf_instances") &&
		(*in.TableName)[len(*in.TableName)-len("wf_instances"):] == "wf_instances" {
		id := ""
		if v, ok := in.Key["id"]; ok {
			id = fromS(v)
		}
		status := f.instanceStatus
		if status == "" {
			status = "running"
		}
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
			"id": avS(id), "status": avS(status), "queue": avS("default"),
		}}, nil
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (f *fakeRecoverStore) PutItem(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.puts++
	return &dynamodb.PutItemOutput{}, nil
}

func recoverIDs(prefix string, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}
	return ids
}

// A single page holding more than bound running instances must still advance
// the cursor: the next pass resumes after the last checked instance instead
// of re-scanning the same page forever.
func TestRecoverOrphanedPass_AdvancesCursorWhenPageExceedsBound(t *testing.T) {
	ctx := context.Background()
	ids := recoverIDs("rec", recoverBound+3)
	f := &fakeRecoverStore{ids: ids} // single page
	b := &Backend{prefix: "tasuki_"}

	rec, next, exhausted, err := b.recoverOrphanedPass(ctx, f, nil, recoverBound)
	if err != nil {
		t.Fatalf("pass1: %v", err)
	}
	if rec != recoverBound {
		t.Fatalf("pass1 recovered=%d want %d", rec, recoverBound)
	}
	if exhausted {
		t.Fatal("pass1 exhausted=true want false (bound overflow)")
	}
	if next == nil {
		t.Fatal("pass1 cursor did not advance (nil); next pass would re-scan the same page forever")
	}
	if got := fromS(next["id"]); got != ids[recoverBound-1] {
		t.Fatalf("pass1 cursor=%q want last checked %q", got, ids[recoverBound-1])
	}

	rec2, _, exhausted2, err := b.recoverOrphanedPass(ctx, f, next, recoverBound)
	if err != nil {
		t.Fatalf("pass2: %v", err)
	}
	if rec2 != 3 {
		t.Fatalf("pass2 recovered=%d want 3 (page tail)", rec2)
	}
	if !exhausted2 {
		t.Fatal("pass2 exhausted=false want true")
	}
	if len(f.scanStarts) != 2 {
		t.Fatalf("scans=%v want 2 passes", f.scanStarts)
	}
	if f.scanStarts[1] != ids[recoverBound-1] {
		t.Fatalf("pass2 resumed at %q want %q", f.scanStarts[1], ids[recoverBound-1])
	}
	if rec+rec2 != len(ids) {
		t.Fatalf("total recovered=%d want %d (every orphan visited once)", rec+rec2, len(ids))
	}
}

// Bound overflow in the middle of a later page resumes mid-page, not at the
// page start (which would redundantly recheck the page head each pass).
func TestRecoverOrphanedPass_ResumesMidPageAcrossPages(t *testing.T) {
	ctx := context.Background()
	ids := recoverIDs("page", recoverBound+5)
	f := &fakeRecoverStore{ids: ids, pageSize: 150}
	b := &Backend{prefix: "tasuki_"}

	rec, next, exhausted, err := b.recoverOrphanedPass(ctx, f, nil, recoverBound)
	if err != nil {
		t.Fatalf("pass1: %v", err)
	}
	if rec != recoverBound || exhausted {
		t.Fatalf("pass1 recovered=%d exhausted=%v want %d/false", rec, exhausted, recoverBound)
	}
	if got := fromS(next["id"]); got != ids[recoverBound-1] {
		t.Fatalf("pass1 cursor=%q want %q", got, ids[recoverBound-1])
	}

	rec2, _, exhausted2, err := b.recoverOrphanedPass(ctx, f, next, recoverBound)
	if err != nil {
		t.Fatalf("pass2: %v", err)
	}
	if rec2 != 5 || !exhausted2 {
		t.Fatalf("pass2 recovered=%d exhausted=%v want 5/true", rec2, exhausted2)
	}
}

// When earlier pages exactly fill the bound, nothing was checked in the
// current page yet, so the cursor falls back to the page start (which already
// advanced past the filled pages).
func TestRecoverOrphanedPass_FallsBackToPageStartAtPageBoundary(t *testing.T) {
	ctx := context.Background()
	ids := recoverIDs("edge", recoverBound+2)
	f := &fakeRecoverStore{ids: ids, pageSize: recoverBound}
	b := &Backend{prefix: "tasuki_"}

	rec, next, exhausted, err := b.recoverOrphanedPass(ctx, f, nil, recoverBound)
	if err != nil {
		t.Fatalf("pass1: %v", err)
	}
	if rec != recoverBound || exhausted {
		t.Fatalf("pass1 recovered=%d exhausted=%v want %d/false", rec, exhausted, recoverBound)
	}
	// First page held exactly bound instances, so the next pass starts at the
	// second page (LastEvaluatedKey of the first page).
	if got := fromS(next["id"]); got != ids[recoverBound-1] {
		t.Fatalf("pass1 cursor=%q want page start %q", got, ids[recoverBound-1])
	}

	rec2, _, exhausted2, err := b.recoverOrphanedPass(ctx, f, next, recoverBound)
	if err != nil {
		t.Fatalf("pass2: %v", err)
	}
	if rec2 != 2 || !exhausted2 {
		t.Fatalf("pass2 recovered=%d exhausted=%v want 2/true", rec2, exhausted2)
	}
}

// TestRecoverOrphanedPass_SkipsTerminalInstance covers issue #291 round-17
// P2(b): the scan image may predate a terminal commit whose sweep already
// finished, and a bare PutItem would then recreate the workflow task after
// the cleanup. The gated put re-checks the live instance status immediately
// before the Put and skips a terminal instance.
func TestRecoverOrphanedPass_SkipsTerminalInstance(t *testing.T) {
	ctx := context.Background()
	ids := recoverIDs("term", 3)
	f := &fakeRecoverStore{ids: ids, instanceStatus: "terminated"}
	b := &Backend{prefix: "tasuki_"}

	rec, _, exhausted, err := b.recoverOrphanedPass(ctx, f, nil, recoverBound)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if rec != 0 {
		t.Fatalf("recovered=%d want 0 (terminal instances must not be recreated)", rec)
	}
	if !exhausted {
		t.Fatal("exhausted=false want true")
	}
	if f.puts != 0 {
		t.Fatalf("puts=%d want 0 (no bare PutItem after a terminal sweep)", f.puts)
	}
}

// transactRecoverStore extends fakeRecoverStore with TransactWriteItems so
// the atomic status-conditioned path of recoverPutIfRunning is exercised:
// the transaction commits only while the instance is running.
type transactRecoverStore struct {
	fakeRecoverStore
	transacts int
	status    string
}

func (f *transactRecoverStore) TransactWriteItems(_ context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.transacts++
	if f.status != "" && f.status != "running" {
		return nil, &types.TransactionCanceledException{
			CancellationReasons: []types.CancellationReason{{Code: recoverStrPtr("ConditionalCheckFailed")}},
		}
	}
	f.puts++
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func recoverStrPtr(s string) *string { return &s }

// TestRecoverPutIfRunning_UsesTransactGate covers the atomic path of issue
// #291 round-17 P2(b): when the store supports transactions the recovery put
// rides the same ConditionCheck(status=running)+Put transaction as
// putWorkflowTaskIfRunning, so a terminal instance aborts instead of
// recreating the row.
func TestRecoverPutIfRunning_UsesTransactGate(t *testing.T) {
	ctx := context.Background()
	b := &Backend{prefix: "tasuki_"}

	running := &transactRecoverStore{status: "running"}
	ok, err := b.recoverPutIfRunning(ctx, running, "inst-run", "default")
	if err != nil || !ok {
		t.Fatalf("running put ok=%v err=%v, want true/nil", ok, err)
	}
	if running.transacts != 1 {
		t.Fatalf("transacts=%d want 1 (atomic gate, not bare PutItem)", running.transacts)
	}

	terminal := &transactRecoverStore{status: "terminated"}
	ok, err = b.recoverPutIfRunning(ctx, terminal, "inst-term", "default")
	if err != nil || ok {
		t.Fatalf("terminal put ok=%v err=%v, want false/nil (gate aborts)", ok, err)
	}
	if terminal.transacts != 1 {
		t.Fatalf("transacts=%d want 1 (gate attempted, then aborted)", terminal.transacts)
	}
	if terminal.puts != 0 {
		t.Fatalf("puts=%d want 0 (terminal row must not be recreated)", terminal.puts)
	}
}

// TestReleaseClaimedLeases_ReleasesBatchOnStatusReadFailure covers issue #291
// round-17 P2(a) at the helper level: a throttled post-claim status read must
// not abandon already-committed leases for a full lease duration. Releases
// are fenced on the claim token, so only the claimed generation matches.
func TestReleaseClaimedLeases_ReleasesBatchOnStatusReadFailure(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	tasks := []backend.Task{
		{ID: 11, Kind: "activity", WorkerID: "w1", Attempt: 3},
		{ID: 12, Kind: "activity", WorkerID: "w1", Attempt: 4},
	}
	b.releaseClaimedLeases(context.Background(), tasks)
	if got := atomic.LoadInt64(&f.updateCalls); got != int64(len(tasks)) {
		t.Fatalf("UpdateItem calls = %d, want %d (one fenced release per claimed task)", got, len(tasks))
	}
	if got := atomic.LoadInt64(&f.updateCalls); got == 0 {
		t.Fatal("no releases issued; failed claims would hide the batch for a full lease")
	}
}
