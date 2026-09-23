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

// wakeWriteTimeout bounds every asynchronous wf_wake UpdateItem: the
// debounce timer callbacks and the synchronous Close flush share this
// budget. Timer callbacks run on their own goroutines tracked by wakeWG,
// which Close waits on, so an unbounded callback write would block Close
// indefinitely; the timeout keeps Close bounded even when DynamoDB stalls.
// Overridden in tests.
var wakeWriteTimeout = 5 * time.Second

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
//
// Once Close begins (wakeClosed set at Close entry), no new debounce state
// is created: the wake is written synchronously and best-effort with a
// wakeWriteTimeout-bound context instead of being dropped with the stopped
// timers. Post-Close wakes deliberately bypass coalescing — each one writes
// — so a mutation racing Close still lands on fast exit. The synchronous
// path touches no shared wake state, so it never interferes with the
// in-flight flush or its wakeWG wait.
func (b *Backend) touchWake(ctx context.Context, pk, instanceID string) {
	_ = ctx
	if b.wakeClosed.Load() {
		b.writeWakeBounded(pk, instanceID)
		return
	}
	d := b.wakeDebounceOrDefault()
	if d <= 0 {
		b.writeWake(context.Background(), pk, instanceID)
		return
	}
	key := pk + "\x00" + instanceID
	b.wakeMu.Lock()
	if b.wakeClosed.Load() {
		// Lost the race with Close after the fast-path check: Close has
		// set the flag and either already snapshotted (an entry created
		// here would be lost) or is about to. Fall back to the
		// synchronous path without touching shared state. The lock
		// serializes this recheck against the flush snapshot, so every
		// touchWake is either flushed by Close or written here: none is
		// dropped. It also keeps wakeWG.Add clear of the flush's Wait,
		// which must never observe an Add once its counter is zero.
		b.wakeMu.Unlock()
		b.writeWakeBounded(pk, instanceID)
		return
	}
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
		// Bounded write (same budget as the synchronous Close flush):
		// Close waits on wakeWG, so an unbounded context here would let
		// a stalled UpdateItem block Close indefinitely.
		ctx, cancel := context.WithTimeout(context.Background(), wakeWriteTimeout)
		defer cancel()
		b.writeWake(ctx, ent.pk, ent.instanceID)
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

// writeWakeBounded performs one best-effort wf_wake UpdateItem with the
// shared wakeWriteTimeout budget. Used by the post-Close touchWake path so a
// stalled DynamoDB cannot hang the mutating goroutine, mirroring the bound
// on timer-callback and flush writes. Errors are ignored like everywhere
// else on this path: wakes are hints, not commits.
func (b *Backend) writeWakeBounded(pk, instanceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), wakeWriteTimeout)
	defer cancel()
	b.writeWake(ctx, pk, instanceID)
}

// flushPendingWakes synchronously writes every debounced wake still waiting
// in the current window. Called by Close so a short-lived process that exits
// right after a mutation still delivers its cross-process wf_wake update
// instead of dropping it with the stopped timer. Best-effort (write errors
// are ignored, like the timer path) and idempotent: a second call finds no
// pending entries. Wakes racing the flush after wakeClosed is set bypass the
// debounce maps entirely and write synchronously from their own goroutine,
// so every wake is either flushed here or written there: none is dropped. A
// timer callback that fired at the boundary (entry already removed, writeWake
// still in flight) is waited on via wakeWG so Close never returns before its
// write completes. Every write — the synchronous flush and each boundary
// callback — carries a wakeWriteTimeout bound and the callbacks run
// concurrently, so Close returns within roughly one budget even when
// DynamoDB stalls instead of blocking indefinitely.
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
		ctx, cancel := context.WithTimeout(context.Background(), wakeWriteTimeout)
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
