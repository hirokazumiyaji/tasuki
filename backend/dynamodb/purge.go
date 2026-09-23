package dynamodb

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/backend"
)

// PurgeInstances removes terminal instances older than the retention window
// together with their tasks, timers, dedupe entries, inbox items, journal and
// inbox sequence counters.
// DynamoDB has no cross-table transaction, so deletion is best-effort per
// instance (the same model as the existing Terminate path).
func (b *Backend) PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error) {
	sts, lim, err := backend.ValidatePurgeArgs(olderThan, statuses, limit)
	if err != nil {
		return 0, err
	}
	cutoff := timeToN(nowUTC().Add(-olderThan))

	names := map[string]string{"#status": "status", "#completed_at": "completed_at"}
	fexpr := "#status IN (" + placeholderList(len(sts), "st") +
		") AND attribute_exists(#completed_at) AND #completed_at <= :cutoff"
	values := map[string]types.AttributeValue{":cutoff": avN(cutoff)}
	for i, s := range sts {
		values[":st"+strconv.Itoa(i)] = avS(s)
	}

	var ids []string
	var startKey map[string]types.AttributeValue
	for {
		out, err := b.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:                 aws.String(b.table("wf_instances")),
			FilterExpression:          aws.String(fexpr),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return 0, err
		}
		for _, m := range out.Items {
			ids = append(ids, fromS(m["id"]))
			if len(ids) >= lim {
				break
			}
		}
		if out.LastEvaluatedKey == nil || len(ids) >= lim {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	purged := 0
	for _, id := range ids {
		if err := b.deleteTasksForInstance(ctx, id); err != nil {
			return purged, err
		}
		if err := b.deleteTimersForInstance(ctx, id); err != nil {
			return purged, err
		}
		if err := b.deleteSignalDedupeForInstance(ctx, id); err != nil {
			return purged, err
		}
		if err := b.deleteInboxForInstance(ctx, id); err != nil {
			return purged, err
		}
		if err := b.deleteJournalForInstance(ctx, id); err != nil {
			return purged, err
		}
		// The per-instance inbox sequence counter is keyed by instance ID,
		// not queried by instance_id; remove it so retention leaves nothing
		// behind and a recreated ID starts from a fresh sequence. Deleting a
		// missing item is a no-op.
		if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(b.table("wf_inbox_seq")),
			Key:       map[string]types.AttributeValue{"id": avS(id)},
		}); err != nil {
			return purged, err
		}
		if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(b.table("wf_instances")),
			Key:       map[string]types.AttributeValue{"id": avS(id)},
		}); err != nil {
			return purged, err
		}
		purged++
	}
	return purged, nil
}

// deleteInboxForInstance / deleteJournalForInstance page over the instance_id
// hash key and remove every item.
func (b *Backend) deleteInboxForInstance(ctx context.Context, id string) error {
	return b.deleteByInstance(ctx, "wf_inbox", id)
}

func (b *Backend) deleteJournalForInstance(ctx context.Context, id string) error {
	return b.deleteByInstance(ctx, "wf_journal", id)
}

func (b *Backend) deleteByInstance(ctx context.Context, name, id string) error {
	table := b.table(name)
	var startKey map[string]types.AttributeValue
	for {
		out, err := b.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String("instance_id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": avS(id)},
			// Terminal cleanup must observe just-committed rows: an
			// eventually-consistent read can miss an inbox row committed
			// right before the terminal transition, reporting success while
			// leaving leftovers no later pass revisits.
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return err
		}
		for _, m := range out.Items {
			key := keyFromItem(name, m)
			if key == nil {
				continue
			}
			if _, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(table), Key: key}); err != nil {
				return err
			}
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func placeholderList(n int, prefix string) string {
	out := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ", "
		}
		out += ":" + prefix + strconv.Itoa(i)
	}
	return out
}
