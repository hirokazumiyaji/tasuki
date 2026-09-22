package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend/hub"
)

// instanceGSIName indexes wf_tasks by instance_id so terminal cleanup and
// TerminateInstance can remove one instance's tasks with an instance-keyed
// Query instead of a full-table Scan (completion cost stays proportional to
// the instance's rows, not the fleet's queued work). Tables created before
// this index existed fall back to Scan (see isMissingIndexError); Migrate
// backfills the index on existing tables (see ensureMissingGSIs).
const instanceGSIName = "instance_gsi"

// gsiBackfillWait caps how long Migrate waits for a newly created GSI to
// become ACTIVE. Queries against a CREATING index fail, so callers keep the
// Scan fallback (isMissingIndexError) until the index is usable; the wait
// only avoids returning before a just-created index converges in the common
// case. A timeout does not fail migration: correctness never depends on the
// index (the fallback is strongly consistent).
const gsiBackfillWait = 60 * time.Second

// Backend is the DynamoDB implementation of backend.Backend.
type Backend struct {
	client *dynamodb.Client
	prefix string
	hub    *hub.Hub

	// recoverMu/recoverCursor rotate orphan-recovery scans across passes
	// so large fleets are eventually fully visited.
	recoverMu     sync.Mutex
	recoverCursor map[string]types.AttributeValue
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

func (b *Backend) Close() error { return nil }

func (b *Backend) Client() *dynamodb.Client { return b.client }

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
		if err := b.ensureMissingGSIs(ctx, d, desc); err != nil {
			return err
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
	waiter := dynamodb.NewTableExistsWaiter(b.client)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(d.name)}, 60*time.Second)
}

// ensureMissingGSIs creates indexes from the table definition that an
// existing table lacks (e.g. instance_gsi on wf_tasks tables created before
// the index existed). DynamoDB allows one GSI creation per UpdateTable call,
// so missing indexes are created sequentially with a bounded wait for ACTIVE
// between them. A wait timeout does not fail migration: terminal cleanup
// keeps the strongly-consistent Scan fallback until the index is usable.
func (b *Backend) ensureMissingGSIs(ctx context.Context, d tableDef, desc *dynamodb.DescribeTableOutput) error {
	if len(d.gsi) == 0 {
		return nil
	}
	existing := make(map[string]types.IndexStatus, len(desc.Table.GlobalSecondaryIndexes))
	for _, gsi := range desc.Table.GlobalSecondaryIndexes {
		existing[aws.ToString(gsi.IndexName)] = gsi.IndexStatus
	}
	for _, want := range d.gsi {
		name := aws.ToString(want.IndexName)
		if status, ok := existing[name]; ok {
			if status != types.IndexStatusActive {
				b.waitGSIActive(ctx, d.name, name)
			}
			continue
		}
		_, uerr := b.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:            aws.String(d.name),
			AttributeDefinitions: attrDefsForGSI(d.attrs, want.KeySchema),
			GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
				{Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName:  want.IndexName,
					KeySchema:  want.KeySchema,
					Projection: want.Projection,
				}},
			},
		})
		if uerr != nil {
			if isGSIAlreadyExistsError(uerr) {
				b.waitGSIActive(ctx, d.name, name)
				continue
			}
			return fmt.Errorf("dynamodb create index %s on %s: %w", name, d.name, uerr)
		}
		b.waitGSIActive(ctx, d.name, name)
	}
	return nil
}

// waitGSIActive polls DescribeTable until the index is ACTIVE or the bound
// elapses. Only context cancellation aborts early; a timeout is a silent
// success because callers fall back to Scan until the index is usable.
func (b *Backend) waitGSIActive(ctx context.Context, table, index string) {
	deadline := time.Now().Add(gsiBackfillWait)
	for {
		if desc, err := b.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		}); err == nil {
			for _, gsi := range desc.Table.GlobalSecondaryIndexes {
				if aws.ToString(gsi.IndexName) == index && gsi.IndexStatus == types.IndexStatusActive {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// attrDefsForGSI selects the attribute definitions covering a GSI key schema
// (UpdateTable creations must declare exactly the indexed attributes).
func attrDefsForGSI(attrs []types.AttributeDefinition, keys []types.KeySchemaElement) []types.AttributeDefinition {
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[aws.ToString(k.AttributeName)] = true
	}
	var out []types.AttributeDefinition
	for _, a := range attrs {
		if want[aws.ToString(a.AttributeName)] {
			out = append(out, a)
		}
	}
	return out
}

// isGSIAlreadyExistsError reports a lost race with another migrator creating
// the same index (create-then-wait is idempotent across processes).
func isGSIAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var riue *types.ResourceInUseException
	if errors.As(err, &riue) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "being created")
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
