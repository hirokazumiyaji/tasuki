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

// instanceGSIName indexes wf_tasks by instance_id so Terminate/Purge can
// delete one instance's tasks with a Query instead of a full-table Scan.
const instanceGSIName = "instance_gsi"

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
	wakeMu       sync.Mutex
	wakePending  map[string]wakeEntry
	wakeTimers   map[string]*time.Timer
	wakeDebounce time.Duration
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

func (b *Backend) Close() error { return nil }

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
	existing := map[string]bool{}
	for _, g := range desc.Table.GlobalSecondaryIndexes {
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
		return nil
	}
	_, err := b.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:                    aws.String(d.name),
		AttributeDefinitions:         d.attrs,
		GlobalSecondaryIndexUpdates: updates,
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			return nil
		}
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("dynamodb create index %s: %w", d.name, err)
	}
	return b.waitForGSIsActive(ctx, d.name, d.gsi)
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

func (b *Backend) waitForGSIsActive(ctx context.Context, name string, want []types.GlobalSecondaryIndex) error {
	deadline := time.Now().Add(60 * time.Second)
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
