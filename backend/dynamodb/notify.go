package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	wakePKTasks    = "tasks"
	wakePKTerminal = "terminal"
)

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	out := make(chan struct{}, 1)
	hubCh, err := b.hub.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	go fanInStruct(ctx, out, hubCh)
	// Poll shared wake item so a different Backend process can wake this subscriber.
	// Table has DynamoDB Streams enabled for external consumers / future stream poller.
	go b.pollWakeItem(ctx, wakePKTasks, func(string) {
		select {
		case out <- struct{}{}:
		default:
		}
	})
	return out, nil
}

func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	out := make(chan string, 1)
	hubCh, err := b.hub.SubscribeTerminal(ctx)
	if err != nil {
		return nil, err
	}
	go fanInString(ctx, out, hubCh)
	go b.pollWakeItem(ctx, wakePKTerminal, func(id string) {
		if id == "" {
			return
		}
		select {
		case out <- id:
		default:
		}
	})
	return out, nil
}

func (b *Backend) notifyTasks() {
	b.hub.NotifyTasks()
	b.touchWake(context.Background(), wakePKTasks, "")
}

func (b *Backend) notifyTerminal(instanceID string) {
	b.hub.NotifyTerminal(instanceID)
	b.touchWake(context.Background(), wakePKTerminal, instanceID)
}

func (b *Backend) touchWake(ctx context.Context, pk, instanceID string) {
	values := map[string]types.AttributeValue{":one": avN(1)}
	update := "ADD n :one"
	if instanceID != "" {
		update += " SET id = :id"
		values[":id"] = avS(instanceID)
	}
	_, _ = b.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(b.table("wf_wake")),
		Key:       map[string]types.AttributeValue{"pk": avS(pk)},
		UpdateExpression:          aws.String(update),
		ExpressionAttributeValues: values,
	})
}

func (b *Backend) pollWakeItem(ctx context.Context, pk string, onBump func(id string)) {
	var last int64 = -1
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName:      aws.String(b.table("wf_wake")),
				Key:            map[string]types.AttributeValue{"pk": avS(pk)},
				ConsistentRead: aws.Bool(true),
			})
			if err != nil || out.Item == nil {
				continue
			}
			n := fromN(out.Item["n"])
			id := fromS(out.Item["id"])
			if last >= 0 && n > last {
				onBump(id)
			}
			last = n
		}
	}
}

func fanInStruct(ctx context.Context, out chan<- struct{}, in <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-in:
			if !ok {
				return
			}
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
}

func fanInString(ctx context.Context, out chan<- string, in <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-in:
			if !ok {
				return
			}
			select {
			case out <- id:
			default:
			}
		}
	}
}
