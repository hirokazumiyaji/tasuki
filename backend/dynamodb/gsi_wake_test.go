package dynamodb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
)

// fakeDynamo is a minimal in-memory stub of dynamoClient for unit tests.
type fakeDynamo struct {
	mu sync.Mutex

	queryCalls  int64
	scanCalls   int64
	updateCalls int64
	deleteCalls int64

	lastQueryIndex string

	queryItems []map[string]types.AttributeValue
	queryErr   error
	scanItems  []map[string]types.AttributeValue
	scanErr    error

	deleted []string

	// migrate support
	describeFn    func(ctx context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	createInputs  map[string]*dynamodb.CreateTableInput
	updateInputs  []*dynamodb.UpdateTableInput
	describeCalls int64
}

func (f *fakeDynamo) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	atomic.AddInt64(&f.describeCalls, 1)
	if f.describeFn != nil {
		return f.describeFn(ctx, in)
	}
	return nil, &types.ResourceNotFoundException{Message: aws.String("not found")}
}
func (f *fakeDynamo) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createInputs == nil {
		f.createInputs = map[string]*dynamodb.CreateTableInput{}
	}
	f.createInputs[aws.ToString(in.TableName)] = in
	return &dynamodb.CreateTableOutput{}, nil
}
func (f *fakeDynamo) UpdateTable(ctx context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateInputs = append(f.updateInputs, in)
	return &dynamodb.UpdateTableOutput{}, nil
}
func (f *fakeDynamo) Scan(ctx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	atomic.AddInt64(&f.scanCalls, 1)
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	return &dynamodb.ScanOutput{Items: f.scanItems}, nil
}
func (f *fakeDynamo) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	atomic.AddInt64(&f.queryCalls, 1)
	f.mu.Lock()
	f.lastQueryIndex = aws.ToString(in.IndexName)
	f.mu.Unlock()
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &dynamodb.QueryOutput{Items: f.queryItems}, nil
}
func (f *fakeDynamo) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}
func (f *fakeDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}
func (f *fakeDynamo) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	atomic.AddInt64(&f.deleteCalls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if pk, ok := in.Key["task_pk"]; ok {
		f.deleted = append(f.deleted, fromS(pk))
	}
	return &dynamodb.DeleteItemOutput{}, nil
}
func (f *fakeDynamo) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	atomic.AddInt64(&f.updateCalls, 1)
	return &dynamodb.UpdateItemOutput{}, nil
}
func (f *fakeDynamo) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func newTestBackend(f *fakeDynamo) *Backend {
	return &Backend{client: f, prefix: "tasuki_", hub: hub.New()}
}

func TestDeleteTasksForInstance_UsesQueryNotScan(t *testing.T) {
	f := &fakeDynamo{
		queryItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#inst-1"), "instance_id": avS("inst-1")},
			{"task_pk": avS("ACT#123"), "instance_id": avS("inst-1")},
		},
	}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstance(context.Background(), "inst-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := atomic.LoadInt64(&f.queryCalls); got != 1 {
		t.Fatalf("Query calls = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 0 {
		t.Fatalf("Scan calls = %d, want 0 (Terminate must not Scan wf_tasks)", got)
	}
	if got := atomic.LoadInt64(&f.deleteCalls); got != 2 {
		t.Fatalf("Delete calls = %d, want 2", got)
	}
	f.mu.Lock()
	idx := f.lastQueryIndex
	f.mu.Unlock()
	if idx != instanceGSIName {
		t.Fatalf("Query IndexName = %q, want %q", idx, instanceGSIName)
	}
}

func TestDeleteTasksForInstance_FallsBackToScanWhenGSIMissing(t *testing.T) {
	f := &fakeDynamo{
		queryErr: errors.New("ValidationException: The table does not have the specified index: " + instanceGSIName),
		scanItems: []map[string]types.AttributeValue{
			{"task_pk": avS("WF#inst-9"), "instance_id": avS("inst-9")},
			{"task_pk": avS("WF#other"), "instance_id": avS("other")},
		},
	}
	b := newTestBackend(f)
	if err := b.deleteTasksForInstance(context.Background(), "inst-9"); err != nil {
		t.Fatalf("delete with fallback: %v", err)
	}
	if got := atomic.LoadInt64(&f.queryCalls); got != 1 {
		t.Fatalf("Query calls = %d, want 1 (attempted first)", got)
	}
	if got := atomic.LoadInt64(&f.scanCalls); got != 1 {
		t.Fatalf("Scan calls = %d, want 1 (fallback)", got)
	}
	if got := atomic.LoadInt64(&f.deleteCalls); got != 1 {
		t.Fatalf("Delete calls = %d, want 1 (only matching instance)", got)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 1 || f.deleted[0] != "WF#inst-9" {
		t.Fatalf("deleted = %v, want [WF#inst-9]", f.deleted)
	}
}

func TestTouchWake_DebouncesWrites(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	b.wakeDebounce = 20 * time.Millisecond
	const ops = 10
	for i := 0; i < ops; i++ {
		b.notifyTasks()
	}
	// In-process hub delivery is immediate; cross-process wake write is debounced.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if atomic.LoadInt64(&f.updateCalls) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for debounced wake write")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Allow any stray second window to fire, then assert coalescing.
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt64(&f.updateCalls); got >= ops {
		t.Fatalf("UpdateItem calls = %d, want < %d (debounced)", got, ops)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls = %d, want 1 (10 rapid notifies coalesced)", got)
	}
}

func TestNextPollInterval_Backoff(t *testing.T) {
	if got := nextPollInterval(wakePollBaseInterval); got != 2*wakePollBaseInterval {
		t.Fatalf("first backoff = %v, want %v", got, 2*wakePollBaseInterval)
	}
	cur := wakePollBaseInterval
	for i := 0; i < 10; i++ {
		cur = nextPollInterval(cur)
	}
	if cur != wakePollMaxInterval {
		t.Fatalf("saturated backoff = %v, want %v", cur, wakePollMaxInterval)
	}
}

func TestMigrate_CreatesTasksWithInstanceGSI(t *testing.T) {
	f := &fakeDynamo{}
	tasksActive := func(name string, gsis []types.GlobalSecondaryIndex) *dynamodb.DescribeTableOutput {
		var descs []types.GlobalSecondaryIndexDescription
		for _, g := range gsis {
			descs = append(descs, types.GlobalSecondaryIndexDescription{
				IndexName: g.IndexName, IndexStatus: types.IndexStatusActive,
			})
		}
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
			TableName: aws.String(name), TableStatus: types.TableStatusActive,
			GlobalSecondaryIndexes: descs,
			StreamSpecification:    &types.StreamSpecification{StreamEnabled: aws.Bool(true)},
		}}
	}
	f.describeFn = func(_ context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		name := aws.ToString(in.TableName)
		f.mu.Lock()
		created, ok := f.createInputs[name]
		f.mu.Unlock()
		if ok {
			return tasksActive(name, created.GlobalSecondaryIndexes), nil
		}
		return nil, &types.ResourceNotFoundException{Message: aws.String("not found")}
	}
	b := newTestBackend(f)
	if err := b.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	created, ok := f.createInputs["tasuki_wf_tasks"]
	if !ok {
		t.Fatal("wf_tasks was not created")
	}
	found := false
	for _, g := range created.GlobalSecondaryIndexes {
		if aws.ToString(g.IndexName) == instanceGSIName {
			found = true
			if len(g.KeySchema) != 1 || aws.ToString(g.KeySchema[0].AttributeName) != "instance_id" {
				t.Fatalf("instance_gsi KeySchema = %+v, want instance_id HASH", g.KeySchema)
			}
		}
	}
	if !found {
		t.Fatalf("wf_tasks GSIs missing %q", instanceGSIName)
	}
}

func TestEnsureMissingGSIs_CreatesInstanceGSIOnExistingTable(t *testing.T) {
	f := &fakeDynamo{}
	desc := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
		},
	}}
	calls := 0
	f.describeFn = func(_ context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		calls++
		if calls > 1 {
			// After UpdateTable, report both GSIs active so waitForGSIsActive returns.
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
				TableName: in.TableName, TableStatus: types.TableStatusActive,
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
					{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
					{IndexName: aws.String(instanceGSIName), IndexStatus: types.IndexStatusActive},
				},
			}}, nil
		}
		return desc, nil
	}
	b := newTestBackend(f)
	d := tableDef{
		name: "tasuki_wf_tasks",
		attrs: []types.AttributeDefinition{
			{AttributeName: aws.String("task_pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		gsi: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("claim_gsi"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	if err := b.ensureMissingGSIs(context.Background(), d, desc); err != nil {
		t.Fatalf("ensureMissingGSIs: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.updateInputs) != 1 {
		t.Fatalf("UpdateTable calls = %d, want 1", len(f.updateInputs))
	}
	var created []string
	for _, u := range f.updateInputs[0].GlobalSecondaryIndexUpdates {
		if u.Create != nil {
			created = append(created, aws.ToString(u.Create.IndexName))
		}
	}
	if len(created) != 1 || created[0] != instanceGSIName {
		t.Fatalf("created indexes = %v, want [%q]", created, instanceGSIName)
	}
}
