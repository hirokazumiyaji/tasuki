package dynamodb

import (
	"context"
	"errors"
	"fmt"
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
	updateFn      func(ctx context.Context, in *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error)

	// updateGate, when non-nil, blocks UpdateItem until closed: a test
	// hook to hold a wake write in flight across Close.
	updateGate chan struct{}

	// updateItemHook, when non-nil, runs inside UpdateItem before success
	// is reported: a test hook to stall or record specific wake writes.
	// It must honor ctx like the real client.
	updateItemHook func(ctx context.Context, in *dynamodb.UpdateItemInput)
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
	f.updateInputs = append(f.updateInputs, in)
	fn := f.updateFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
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
	if f.updateGate != nil {
		// Honor ctx like the real client: a stalled write unblocks when
		// the caller's context times out instead of hanging forever.
		select {
		case <-f.updateGate:
		case <-ctx.Done():
		}
	}
	if f.updateItemHook != nil {
		f.updateItemHook(ctx, in)
	}
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
	// UpdateTable must carry definitions only for the new index keys:
	// DynamoDB rejects unrelated definitions (e.g. task_pk) as unused.
	defs := f.updateInputs[0].AttributeDefinitions
	if len(defs) != 1 || aws.ToString(defs[0].AttributeName) != "instance_id" ||
		defs[0].AttributeType != types.ScalarAttributeTypeS {
		t.Fatalf("AttributeDefinitions = %+v, want only [{instance_id S}]", defs)
	}
}

func TestEnsureMissingGSIs_SucceedsWhileGSIBackfilling(t *testing.T) {
	oldTimeout := gsiWaitTimeout
	gsiWaitTimeout = 30 * time.Millisecond
	defer func() { gsiWaitTimeout = oldTimeout }()

	f := &fakeDynamo{}
	backfilling := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
			// New index never leaves CREATING: backfill outlasts the wait budget.
			{IndexName: aws.String(instanceGSIName), IndexStatus: types.IndexStatusCreating},
		},
	}}
	f.describeFn = func(_ context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		return backfilling, nil
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
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	desc := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
		},
	}}
	// Slow backfill must not fail Migrate: callers fall back to Scan.
	if err := b.ensureMissingGSIs(context.Background(), d, desc); err != nil {
		t.Fatalf("ensureMissingGSIs during backfill = %v, want nil (Scan fallback covers readers)", err)
	}
}

func TestEnsureMissingGSIs_ContextCancelStillAborts(t *testing.T) {
	oldTimeout := gsiWaitTimeout
	gsiWaitTimeout = time.Hour // deadline never hit; cancellation must abort
	defer func() { gsiWaitTimeout = oldTimeout }()

	f := &fakeDynamo{}
	backfilling := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String(instanceGSIName), IndexStatus: types.IndexStatusCreating},
		},
	}}
	f.describeFn = func(_ context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		return backfilling, nil
	}
	b := newTestBackend(f)
	d := tableDef{
		name: "tasuki_wf_tasks",
		gsi: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	desc := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.ensureMissingGSIs(ctx, d, desc); err == nil {
		t.Fatal("ensureMissingGSIs with cancelled context = nil, want context.Canceled")
	}
}

func TestClose_FlushesPendingWake(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	// Hour-long window: the timer cannot fire on its own, so any write
	// must come from the Close flush.
	b.wakeDebounce = time.Hour
	b.notifyTasks()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls after Close = %d, want 1 (pending wake flushed)", got)
	}
	// Idempotent: no pending entries left, no further writes.
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls after second Close = %d, want 1", got)
	}
}

func TestClose_FlushesDistinctTerminalWakes(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	b.wakeDebounce = time.Hour
	b.notifyTerminal("inst-a")
	b.notifyTerminal("inst-b")
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 2 {
		t.Fatalf("UpdateItem calls after Close = %d, want 2 (one per instance)", got)
	}
}

func TestClose_TouchWakeAfterCloseWritesSynchronously(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	// Hour-long window: no timer can fire on its own, so any write after
	// Close must come from the synchronous post-Close path.
	b.wakeDebounce = time.Hour
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b.notifyTasks()
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("post-Close wake was dropped instead of written synchronously")
		}
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls = %d, want 1 (post-Close wake writes once)", got)
	}
	// No debounce state may be recreated after Close: no maps, no timers.
	b.wakeMu.Lock()
	pendingNil := b.wakePending == nil
	timers := len(b.wakeTimers)
	b.wakeMu.Unlock()
	if !pendingNil || timers != 0 {
		t.Fatalf("post-Close touchWake recreated debounce state: pendingNil=%v timers=%d", pendingNil, timers)
	}
	// A second post-Close wake also lands (no coalescing once closed).
	b.notifyTerminal("post-close-a")
	deadline = time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("second post-Close wake was dropped")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClose_MutationRacingCloseIsAccounted(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	b.wakeDebounce = time.Hour
	// Distinct terminal keys never coalesce, so every racing mutation
	// must produce exactly one write: either flushed by Close (pre-close
	// snapshot) or written synchronously by touchWake (post-Close flag).
	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.notifyTerminal(fmt.Sprintf("race-%d", i))
		}(i)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) != n {
		if time.Now().After(deadline) {
			t.Fatalf("UpdateItem calls = %d, want %d (every racing wake must land)", atomic.LoadInt64(&f.updateCalls), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClose_WithoutPendingWakeIsNoop(t *testing.T) {
	f := &fakeDynamo{}
	b := newTestBackend(f)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 0 {
		t.Fatalf("UpdateItem calls = %d, want 0", got)
	}
}

func TestClose_WaitsForInflightWakeCallback(t *testing.T) {
	gate := make(chan struct{})
	f := &fakeDynamo{updateGate: gate}
	b := newTestBackend(f)
	b.wakeDebounce = time.Millisecond
	b.notifyTasks()
	// Wait until the timer callback has entered writeWake: its entry is
	// already removed from the pending maps while the write is blocked
	// on the gate. This is the debounce-boundary state where the old
	// flush saw an empty snapshot and returned early.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for wake callback to start")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{})
	go func() {
		_ = b.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a wake callback write was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the in-flight wake write completed")
	}
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls = %d, want 1", got)
	}
}

// TestClose_WaitsForPostCloseWakeWrite pins the post-Close tracking: a
// synchronous post-Close write in flight across Close must be observed by
// Close instead of dropped on fast exit. Without wakePostClose tracking, the
// flush snapshots empty debounce maps and Waits on a zero wakeWG while the
// write is still in flight, so the second Close below would return early.
func TestClose_WaitsForPostCloseWakeWrite(t *testing.T) {
	gate := make(chan struct{})
	f := &fakeDynamo{updateGate: gate}
	b := newTestBackend(f)
	b.wakeDebounce = time.Hour
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Post-Close wake starts in the background and blocks inside UpdateItem.
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		b.notifyTasks()
	}()
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("post-Close wake never entered UpdateItem")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{})
	go func() {
		_ = b.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a post-Close wake write was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the post-Close wake write completed")
	}
	<-wrote
	if got := atomic.LoadInt64(&f.updateCalls); got != 1 {
		t.Fatalf("UpdateItem calls = %d, want 1", got)
	}
}

// TestClose_FlushDoesNotLetStalledWriteSuppressOthers pins the per-write
// flush budgets: the flush fires every pending wake concurrently, each with
// its own live wakeWriteTimeout context, so one stalled UpdateItem cannot
// consume a shared context and suppress the rest. The hook models the wire
// faithfully: a write whose context already expired never lands, and the
// stalled write holds only until its own budget expires.
func TestClose_FlushDoesNotLetStalledWriteSuppressOthers(t *testing.T) {
	old := wakeWriteTimeout
	wakeWriteTimeout = 200 * time.Millisecond
	defer func() { wakeWriteTimeout = old }()

	f := &fakeDynamo{}
	var mu sync.Mutex
	landed := map[string]int{}
	f.updateItemHook = func(ctx context.Context, in *dynamodb.UpdateItemInput) {
		if ctx.Err() != nil {
			return // budget already expired: the write never lands
		}
		pk := fromS(in.Key["pk"])
		if pk == wakePKTasks {
			// Stalled write: hold until this write's own budget expires.
			<-ctx.Done()
			return
		}
		id := ""
		if v, ok := in.ExpressionAttributeValues[":id"]; ok {
			id = fromS(v)
		}
		mu.Lock()
		landed[pk+"\x00"+id]++
		mu.Unlock()
	}
	b := newTestBackend(f)
	b.wakeDebounce = time.Hour
	b.notifyTasks()            // pk=tasks (stalled)
	b.notifyTerminal("victim") // pk=terminal (must still land)
	start := time.Now()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	mu.Lock()
	got := landed[wakePKTerminal+"\x00victim"]
	mu.Unlock()
	if got != 1 {
		t.Fatalf("stalled tasks wake suppressed the terminal wake: landed=%v", landed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Close blocked %v, want roughly one wakeWriteTimeout budget", elapsed)
	}
}

// TestClose_BoundedWhenWakeWriteStalls pins the Close bound: a debounce
// timer callback whose UpdateItem never returns (unresponsive DynamoDB)
// must not block Close past the wake write budget.
func TestClose_BoundedWhenWakeWriteStalls(t *testing.T) {
	old := wakeWriteTimeout
	wakeWriteTimeout = 50 * time.Millisecond
	defer func() { wakeWriteTimeout = old }()
	// Gate never closes: the wake write stalls until its context times out.
	f := &fakeDynamo{updateGate: make(chan struct{})}
	b := newTestBackend(f)
	b.wakeDebounce = time.Millisecond
	b.notifyTasks()
	// Wait until the timer callback has entered UpdateItem.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&f.updateCalls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for wake callback to start")
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Close blocked %v on a stalled wake write, want bounded by wakeWriteTimeout", elapsed)
	}
}

func TestEnsureMissingGSIs_RetriesAfterResourceInUse(t *testing.T) {
	oldRetryTimeout := gsiUpdateRetryTimeout
	oldRetryInterval := gsiUpdateRetryInterval
	oldWait := gsiWaitTimeout
	gsiUpdateRetryTimeout = 2 * time.Second
	gsiUpdateRetryInterval = time.Millisecond
	gsiWaitTimeout = 2 * time.Second
	defer func() {
		gsiUpdateRetryTimeout = oldRetryTimeout
		gsiUpdateRetryInterval = oldRetryInterval
		gsiWaitTimeout = oldWait
	}()

	f := &fakeDynamo{}
	var updateCalls int64
	f.updateFn = func(_ context.Context, _ *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
		if atomic.AddInt64(&updateCalls, 1) == 1 {
			// Table busy with an unrelated update: index NOT created.
			return nil, &types.ResourceInUseException{Message: aws.String("table updating")}
		}
		return &dynamodb.UpdateTableOutput{}, nil
	}
	missing := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
		},
	}}
	withInstance := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
			{IndexName: aws.String(instanceGSIName), IndexStatus: types.IndexStatusActive},
		},
	}}
	f.describeFn = func(_ context.Context, _ *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		if atomic.LoadInt64(&updateCalls) >= 2 {
			return withInstance, nil
		}
		return missing, nil
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
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	desc := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
		},
	}}
	if err := b.ensureMissingGSIs(context.Background(), d, desc); err != nil {
		t.Fatalf("ensureMissingGSIs after ResourceInUse = %v, want nil (retry then succeed)", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if got := len(f.updateInputs); got != 2 {
		t.Fatalf("UpdateTable calls = %d, want 2 (initial ResourceInUse + retry)", got)
	}
}

func TestEnsureMissingGSIs_ResourceInUseBudgetFallsBackToScan(t *testing.T) {
	oldRetryTimeout := gsiUpdateRetryTimeout
	oldRetryInterval := gsiUpdateRetryInterval
	gsiUpdateRetryTimeout = 30 * time.Millisecond
	gsiUpdateRetryInterval = time.Millisecond
	defer func() {
		gsiUpdateRetryTimeout = oldRetryTimeout
		gsiUpdateRetryInterval = oldRetryInterval
	}()

	f := &fakeDynamo{}
	var updateCalls int64
	f.updateFn = func(_ context.Context, _ *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
		atomic.AddInt64(&updateCalls, 1)
		return nil, &types.ResourceInUseException{Message: aws.String("table updating")}
	}
	missing := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			{IndexName: aws.String("claim_gsi"), IndexStatus: types.IndexStatusActive},
		},
	}}
	f.describeFn = func(_ context.Context, _ *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		return missing, nil
	}
	b := newTestBackend(f)
	d := tableDef{
		name: "tasuki_wf_tasks",
		attrs: []types.AttributeDefinition{
			{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		gsi: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	// Persistently busy table must not fail Migrate: Scan fallback covers readers.
	if err := b.ensureMissingGSIs(context.Background(), d, missing); err != nil {
		t.Fatalf("ensureMissingGSIs with persistent ResourceInUse = %v, want nil (Scan fallback)", err)
	}
	if got := atomic.LoadInt64(&updateCalls); got < 2 {
		t.Fatalf("UpdateTable calls = %d, want >= 2 (must retry, not treat first ResourceInUse as success)", got)
	}
}

func TestEnsureMissingGSIs_ResourceInUseRespectsCancel(t *testing.T) {
	oldRetryTimeout := gsiUpdateRetryTimeout
	oldRetryInterval := gsiUpdateRetryInterval
	gsiUpdateRetryTimeout = time.Hour
	gsiUpdateRetryInterval = time.Millisecond
	defer func() {
		gsiUpdateRetryTimeout = oldRetryTimeout
		gsiUpdateRetryInterval = oldRetryInterval
	}()

	f := &fakeDynamo{}
	f.updateFn = func(_ context.Context, _ *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
		return nil, &types.ResourceInUseException{Message: aws.String("table updating")}
	}
	f.describeFn = func(_ context.Context, _ *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
			TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusUpdating,
		}}, nil
	}
	b := newTestBackend(f)
	d := tableDef{
		name: "tasuki_wf_tasks",
		attrs: []types.AttributeDefinition{
			{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		gsi: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(instanceGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
	desc := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName: aws.String("tasuki_wf_tasks"), TableStatus: types.TableStatusActive,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.ensureMissingGSIs(ctx, d, desc); err == nil {
		t.Fatal("ensureMissingGSIs with cancelled context = nil, want context.Canceled")
	}
}
