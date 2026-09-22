package dynamodb

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

func (f *fakeRecoverStore) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
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
