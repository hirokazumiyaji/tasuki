package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
)

// instanceGSIName indexes wf_tasks by instance_id so Terminate/Purge can
// delete one instance's tasks with a Query instead of a full-table Scan.
const instanceGSIName = "instance_gsi"

// gsiWaitTimeout bounds the best-effort wait for a newly created GSI to
// become ACTIVE during Migrate. Backfilling a GSI on a large table can take
// far longer; callers fall back to Scan while the index builds, so Migrate
// proceeds (with a warning) instead of failing once the budget is exhausted.
// Overridden in tests.
var gsiWaitTimeout = 60 * time.Second

// gsiUpdateRetryTimeout bounds retries of UpdateTable GSI creation when
// DynamoDB reports ResourceInUse because wf_tasks is undergoing an unrelated
// update (stream/index operation). The index was not created in that case,
// so returning success would leave Terminate/Purge on full-table Scans.
// Instead Migrate waits for the conflicting operation, re-describes, and
// retries. Exhausting the budget falls back to Scan (with a warning) rather
// than failing Migrate. Overridden in tests.
var gsiUpdateRetryTimeout = 60 * time.Second

// gsiUpdateRetryInterval is the poll interval while waiting for a conflicting
// table update to finish before retrying GSI creation. Overridden in tests.
var gsiUpdateRetryInterval = 500 * time.Millisecond

// Backend is the DynamoDB implementation of backend.Backend.
type Backend struct {
	client dynamoClient
	prefix string
	hub    *hub.Hub

	// recoverMu/recoverCursor rotate orphan-recovery scans across passes
	// so large fleets are eventually fully visited.
	recoverMu     sync.Mutex
	recoverCursor map[string]types.AttributeValue

	// wakeMu/wakePending/wakeTimers debounce cross-process wake writes
	// (wf_wake UpdateItem) so bursty operations coalesce into one write.
	// wakeWG tracks in-flight debounce timer callbacks so Close can wait
	// for a callback that already removed its entry but has not yet
	// finished writeWake. Each callback write carries a wakeWriteTimeout
	// bound, so the wait is bounded even when DynamoDB stalls.
	// wakeClosed is set at Close entry, before the flush snapshot: once
	// set, touchWake never creates maps or schedules new timers and
	// instead writes synchronously (best-effort, bounded), so a mutation
	// racing Close still lands instead of being dropped with the stopped
	// timers on fast exit.
	wakeMu       sync.Mutex
	wakePending  map[string]wakeEntry
	wakeTimers   map[string]*time.Timer
	wakeDebounce time.Duration
	wakeWG       sync.WaitGroup
	wakeClosed   atomic.Bool
	// wakePostClose counts synchronous post-Close wake writes currently in
	// flight (see writeWakeCounted). They bypass the debounce maps, so
	// wakeWG cannot track them; flushPendingWakes drains this count before
	// Close returns so a wake racing Close is observed instead of dropped
	// on fast exit. Each write is wakeWriteTimeout-bounded, so the drain is
	// bounded too. An atomic (not wakeWG) is used deliberately:
	// WaitGroup.Add concurrent with Wait panics once the counter is zero,
	// and post-Close writes may start at any moment after wakeClosed is
	// set, including while the flush is already waiting. Registration (the
	// Add) happens under wakeMu, the same mutex the flush's final
	// zero-observation holds, so the two are mutually exclusive.
	wakePostClose atomic.Int64
}

type wakeEntry struct {
	pk         string
	instanceID string
}

// Config holds connection options.
type Config struct {
	Endpoint string // e.g. http://localhost:8000 for DynamoDB Local
	Region   string
	Prefix   string // table name prefix, default "tasuki_"
}

// New opens a DynamoDB client. For Local, set Endpoint and dummy AWS credentials.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Region == "" {
		cfg.Region = envOr("AWS_REGION", "us-east-1")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = envOr("TASUKI_DYNAMODB_TABLE_PREFIX", "tasuki_")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.Endpoint != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				envOr("AWS_ACCESS_KEY_ID", "local"),
				envOr("AWS_SECRET_ACCESS_KEY", "local"),
				"",
			),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: load config: %w", err)
	}

	var clientOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	client := dynamodb.NewFromConfig(awsCfg, clientOpts...)
	return &Backend{client: client, prefix: cfg.Prefix, hub: hub.New()}, nil
}

func (b *Backend) Close() error {
	// Mark closed BEFORE the flush snapshot: touchWake checks this flag
	// (fast path plus a recheck under wakeMu) and switches to synchronous
	// best-effort writes, so no new debounce timers can be scheduled once
	// closing begins and post-Close mutation wakes are never lost with
	// the stopped timers on fast exit.
	b.wakeClosed.Store(true)
	// Flush debounced cross-process wake writes (see flushPendingWakes);
	// otherwise a Close within the debounce window would drop them.
	b.flushPendingWakes()
	return nil
}

func (b *Backend) Client() *dynamodb.Client {
	if c, ok := b.client.(*dynamodb.Client); ok {
		return c
	}
	return nil
}

func (b *Backend) Prefix() string { return b.prefix }

func (b *Backend) table(name string) string { return b.prefix + name }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func (b *Backend) Migrate(ctx context.Context) error {
	defs := []tableDef{
		{
			name: b.table("wf_instances"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
		},
		{
			name: b.table("wf_journal"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("seq"), AttributeType: types.ScalarAttributeTypeN},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("seq"), KeyType: types.KeyTypeRange},
			},
		},
		{
			name: b.table("wf_inbox"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeN},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange},
			},
		},
		{
			name: b.table("wf_inbox_seq"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
		},
		{
			name: b.table("wf_signal_dedupe"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("dedupe_id"), AttributeType: types.ScalarAttributeTypeS},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("dedupe_id"), KeyType: types.KeyTypeRange},
			},
		},
		{
			name: b.table("wf_tasks"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("task_pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("visible_at"), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("task_pk"), KeyType: types.KeyTypeHash},
			},
			gsi: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("claim_gsi"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("visible_at"), KeyType: types.KeyTypeRange},
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
		},
		{
			name: b.table("wf_timers"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("instance_id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("seq"), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("fire_at"), AttributeType: types.ScalarAttributeTypeN},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("instance_id"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("seq"), KeyType: types.KeyTypeRange},
			},
			gsi: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("fire_gsi"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("fire_at"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			name: b.table("wf_schedules"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("next_run_at"), AttributeType: types.ScalarAttributeTypeN},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
			gsi: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("due_gsi"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("next_run_at"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			name: b.table("wf_wake"),
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			keys: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			stream: true,
		},
	}
	for _, d := range defs {
		if err := b.ensureTable(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

type tableDef struct {
	name   string
	attrs  []types.AttributeDefinition
	keys   []types.KeySchemaElement
	gsi    []types.GlobalSecondaryIndex
	stream bool
}

func (b *Backend) ensureTable(ctx context.Context, d tableDef) error {
	desc, err := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(d.name),
	})
	if err == nil {
		if err := b.ensureMissingGSIs(ctx, d, desc); err != nil {
			return err
		}
		if d.stream && (desc.Table.StreamSpecification == nil || !aws.ToBool(desc.Table.StreamSpecification.StreamEnabled)) {
			_, uerr := b.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName: aws.String(d.name),
				StreamSpecification: &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: types.StreamViewTypeNewImage,
				},
			})
			if uerr != nil {
				return fmt.Errorf("dynamodb enable stream %s: %w", d.name, uerr)
			}
		}
		return nil
	}
	var nfe *types.ResourceNotFoundException
	if !errors.As(err, &nfe) {
		return fmt.Errorf("dynamodb describe %s: %w", d.name, err)
	}

	in := &dynamodb.CreateTableInput{
		TableName:            aws.String(d.name),
		AttributeDefinitions: d.attrs,
		KeySchema:            d.keys,
		BillingMode:          types.BillingModePayPerRequest,
	}
	if len(d.gsi) > 0 {
		in.GlobalSecondaryIndexes = d.gsi
	}
	if d.stream {
		in.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		}
	}
	_, err = b.client.CreateTable(ctx, in)
	if err != nil {
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			return nil
		}
		return fmt.Errorf("dynamodb create %s: %w", d.name, err)
	}
	return b.waitForTableActive(ctx, d.name)
}

// ensureMissingGSIs creates GSIs declared in d.gsi but absent from an
// existing table (backward compat for tables created before the GSI was
// added). Callers fall back to Scan when the GSI is still missing, so a
// failed or pending update never breaks Terminate/Purge.
func (b *Backend) ensureMissingGSIs(ctx context.Context, d tableDef, desc *dynamodb.DescribeTableOutput) error {
	if len(d.gsi) == 0 || desc == nil || desc.Table == nil {
		return nil
	}
	// Resolve key attribute types once; the missing set is recomputed
	// after every ResourceInUse retry because a concurrent creator may
	// have added the index while the table was busy.
	attrType := make(map[string]types.ScalarAttributeType, len(d.attrs))
	for _, a := range d.attrs {
		attrType[aws.ToString(a.AttributeName)] = a.AttributeType
	}
	curDesc := desc
	deadline := time.Now().Add(gsiUpdateRetryTimeout)
	for {
		if curDesc == nil || curDesc.Table == nil {
			return fmt.Errorf("dynamodb describe %s: missing table description", d.name)
		}
		existing := map[string]bool{}
		for _, g := range curDesc.Table.GlobalSecondaryIndexes {
			existing[aws.ToString(g.IndexName)] = true
		}
		var updates []types.GlobalSecondaryIndexUpdate
		for _, want := range d.gsi {
			name := aws.ToString(want.IndexName)
			if existing[name] {
				continue
			}
			w := want
			updates = append(updates, types.GlobalSecondaryIndexUpdate{Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: w.IndexName, KeySchema: w.KeySchema, Projection: w.Projection,
			}})
		}
		if len(updates) == 0 {
			break
		}
		// UpdateTable with a GSI create accepts attribute definitions only
		// for the new index key attributes. Passing the full table
		// definitions (e.g. task_pk, gsi_pk, visible_at when only
		// instance_gsi is added) is rejected as unused, so build the
		// definitions from the new indexes' key schemas, resolving types
		// from the table definition.
		var attrDefs []types.AttributeDefinition
		seen := make(map[string]bool, len(updates))
		for _, u := range updates {
			if u.Create == nil {
				continue
			}
			for _, k := range u.Create.KeySchema {
				name := aws.ToString(k.AttributeName)
				if seen[name] {
					continue
				}
				seen[name] = true
				t, ok := attrType[name]
				if !ok {
					t = types.ScalarAttributeTypeS
				}
				attrDefs = append(attrDefs, types.AttributeDefinition{
					AttributeName: aws.String(name), AttributeType: t,
				})
			}
		}
		_, err := b.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:                    aws.String(d.name),
			AttributeDefinitions:         attrDefs,
			GlobalSecondaryIndexUpdates: updates,
		})
		if err == nil {
			break
		}
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			// The table is busy with an unrelated update; the index was
			// NOT created. Wait for the conflicting operation, re-describe,
			// and retry instead of treating this as success (which would
			// leave Terminate/Purge on full-table Scans).
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "dynamodb: index creation on %s deferred (table busy), continuing with Scan fallback: %v\n", d.name, err)
				return nil
			}
			if werr := b.waitForTableReady(ctx, d.name, deadline); werr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				fmt.Fprintf(os.Stderr, "dynamodb: index creation on %s deferred (table busy), continuing with Scan fallback: %v\n", d.name, err)
				return nil
			}
			fresh, derr := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(d.name),
			})
			if derr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Re-describe is best-effort: retry the update; a persistent
				// failure surfaces on the next UpdateTable attempt.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(gsiUpdateRetryInterval):
				}
				continue
			}
			curDesc = fresh
			continue
		}
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("dynamodb create index %s: %w", d.name, err)
	}
	// Best-effort wait: GSI backfill on a large table can take far longer
	// than gsiWaitTimeout while DynamoDB is still successfully building the
	// index. Callers fall back to Scan until the index is ACTIVE, so a slow
	// backfill must not fail Migrate. Only a cancelled context aborts.
	if err := b.waitForGSIsActive(ctx, d.name, d.gsi); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprintf(os.Stderr, "dynamodb: index backfill on %s still pending, continuing with Scan fallback: %v\n", d.name, err)
	}
	return nil
}

func (b *Backend) waitForTableActive(ctx context.Context, name string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		desc, err := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err == nil && desc.Table != nil && desc.Table.TableStatus == types.TableStatusActive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dynamodb wait active %s: timeout", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForTableReady polls DescribeTable until the table leaves its conflicting
// update (TableStatus ACTIVE) so a GSI create rejected with ResourceInUse can
// be retried. It returns nil once the table is ACTIVE, ctx.Err() if the
// context ends, or a timeout error once deadline passes; callers fall back to
// Scan with a warning on timeout.
func (b *Backend) waitForTableReady(ctx context.Context, name string, deadline time.Time) error {
	for {
		desc, err := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err == nil && desc != nil && desc.Table != nil && desc.Table.TableStatus == types.TableStatusActive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dynamodb wait table ready %s: timeout", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gsiUpdateRetryInterval):
		}
	}
}

func (b *Backend) waitForGSIsActive(ctx context.Context, name string, want []types.GlobalSecondaryIndex) error {
	deadline := time.Now().Add(gsiWaitTimeout)
	for {
		desc, err := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err == nil && desc.Table != nil {
			byName := map[string]types.IndexStatus{}
			for _, g := range desc.Table.GlobalSecondaryIndexes {
				byName[aws.ToString(g.IndexName)] = g.IndexStatus
			}
			ready := true
			for _, w := range want {
				if byName[aws.ToString(w.IndexName)] != types.IndexStatusActive {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dynamodb wait index active %s: timeout", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Reset deletes all items from all tables (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	tables := []string{
		b.table("wf_wake"),
		b.table("wf_schedules"),
		b.table("wf_timers"),
		b.table("wf_tasks"),
		b.table("wf_inbox"),
		b.table("wf_inbox_seq"),
		b.table("wf_signal_dedupe"),
		b.table("wf_journal"),
		b.table("wf_instances"),
	}
	for _, name := range tables {
		if err := b.clearTable(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) clearTable(ctx context.Context, name string) error {
	var startKey map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(name),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return fmt.Errorf("dynamodb scan %s: %w", name, err)
		}
		for _, item := range out.Items {
			key := keyFromItem(name, item)
			if key == nil {
				continue
			}
			_, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(name),
				Key:       key,
			})
			if err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func keyFromItem(table string, item map[string]types.AttributeValue) map[string]types.AttributeValue {
	if len(table) >= 7 && table[len(table)-7:] == "wf_wake" {
		if pk, ok := item["pk"]; ok {
			return map[string]types.AttributeValue{"pk": pk}
		}
		return nil
	}
	switch {
	case len(table) >= 8 && table[len(table)-8:] == "wf_tasks":
		return map[string]types.AttributeValue{"task_pk": item["task_pk"]}
	case len(table) >= 10 && table[len(table)-10:] == "wf_journal":
		return map[string]types.AttributeValue{"instance_id": item["instance_id"], "seq": item["seq"]}
	case len(table) >= 9 && table[len(table)-9:] == "wf_timers":
		return map[string]types.AttributeValue{"instance_id": item["instance_id"], "seq": item["seq"]}
	case len(table) >= 8 && table[len(table)-8:] == "wf_inbox":
		return map[string]types.AttributeValue{"instance_id": item["instance_id"], "id": item["id"]}
	case len(table) >= 16 && table[len(table)-16:] == "wf_signal_dedupe":
		return map[string]types.AttributeValue{"instance_id": item["instance_id"], "dedupe_id": item["dedupe_id"]}
	default:
		// wf_instances, wf_schedules
		return map[string]types.AttributeValue{"id": item["id"]}
	}
}
