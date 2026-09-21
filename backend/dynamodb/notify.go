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

	// defaultWakeDebounce coalesces bursty wake writes into one UpdateItem.
	// Kept in the 10-50ms window so cross-process wake latency stays low.
	defaultWakeDebounce = 20 * time.Millisecond
	// wakePollBaseInterval / wakePollMaxInterval bound the quiet-time
	// exponential backoff for cross-process wake polling.
	wakePollBaseInterval = 100 * time.Millisecond
	wakePollMaxInterval  = 1 * time.Second
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

func (b *Backend) wakeDebounceOrDefault() time.Duration {
	b.wakeMu.Lock()
	d := b.wakeDebounce
	b.wakeMu.Unlock()
	if d <= 0 {
		return defaultWakeDebounce
	}
	return d
}

// touchWake schedules a coalesced wf_wake UpdateItem. Bursty callers
// (CreateInstance/CompleteActivity/CommitAdvancements/ReleaseLease/
// SendToInbox/...) collapse into a single write per debounce window.
// Terminal wakes are keyed by instance ID so distinct completions never
// coalesce away each other's payload (the wake item holds one id).
func (b *Backend) touchWake(ctx context.Context, pk, instanceID string) {
	_ = ctx
	d := b.wakeDebounceOrDefault()
	if d <= 0 {
		b.writeWake(context.Background(), pk, instanceID)
		return
	}
	key := pk + "\x00" + instanceID
	b.wakeMu.Lock()
	if b.wakePending == nil {
		b.wakePending = make(map[string]wakeEntry)
		b.wakeTimers = make(map[string]*time.Timer)
	}
	b.wakePending[key] = wakeEntry{pk: pk, instanceID: instanceID}
	if _, ok := b.wakeTimers[key]; ok {
		b.wakeMu.Unlock()
		return
	}
	// Track the pending callback so Close can wait for a timer that
	// fires at the debounce boundary: such a callback removes its entry
	// before Close snapshots the maps, so the flush alone would miss it.
	b.wakeWG.Add(1)
	b.wakeTimers[key] = time.AfterFunc(d, func() {
		defer b.wakeWG.Done()
		b.wakeMu.Lock()
		ent, ok := b.wakePending[key]
		delete(b.wakeTimers, key)
		delete(b.wakePending, key)
		b.wakeMu.Unlock()
		if !ok {
			return
		}
		b.writeWake(context.Background(), ent.pk, ent.instanceID)
	})
	b.wakeMu.Unlock()
}

func (b *Backend) writeWake(ctx context.Context, pk, instanceID string) {
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

// flushPendingWakes synchronously writes every debounced wake still waiting
// in the current window. Called by Close so a short-lived process that exits
// right after a mutation still delivers its cross-process wf_wake update
// instead of dropping it with the stopped timer. Best-effort (write errors
// are ignored, like the timer path) and idempotent: a second call finds no
// pending entries. Wakes scheduled concurrently with the flush land in a
// fresh window and fire on their own timer. A timer callback that fired at
// the boundary (entry already removed, writeWake still in flight) is waited
// on via wakeWG so Close never returns before its write completes.
func (b *Backend) flushPendingWakes() {
	b.wakeMu.Lock()
	pending := b.wakePending
	timers := b.wakeTimers
	b.wakePending = nil
	b.wakeTimers = nil
	b.wakeMu.Unlock()
	for _, t := range timers {
		if t.Stop() {
			// Callback will never run; balance the Add from touchWake.
			// A false return means the callback already started (or
			// finished) and its own Done balances the Add.
			b.wakeWG.Done()
		}
	}
	if len(pending) != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, ent := range pending {
			b.writeWake(ctx, ent.pk, ent.instanceID)
		}
	}
	// Wait for boundary callbacks whose entries were removed before the
	// snapshot above but whose writes had not yet completed.
	b.wakeWG.Wait()
}

func nextPollInterval(cur time.Duration) time.Duration {
	nxt := cur * 2
	if nxt < wakePollBaseInterval {
		nxt = wakePollBaseInterval
	}
	if nxt > wakePollMaxInterval {
		nxt = wakePollMaxInterval
	}
	return nxt
}

func (b *Backend) pollWakeItem(ctx context.Context, pk string, onBump func(id string)) {
	var last int64 = -1
	interval := wakePollBaseInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName:      aws.String(b.table("wf_wake")),
				Key:            map[string]types.AttributeValue{"pk": avS(pk)},
				ConsistentRead: aws.Bool(true),
			})
			if err != nil || out.Item == nil {
				interval = nextPollInterval(interval)
				timer.Reset(interval)
				continue
			}
			n := fromN(out.Item["n"])
			id := fromS(out.Item["id"])
			bumped := last >= 0 && n > last
			last = n
			if bumped {
				onBump(id)
				interval = wakePollBaseInterval
			} else {
				interval = nextPollInterval(interval)
			}
			timer.Reset(interval)
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
