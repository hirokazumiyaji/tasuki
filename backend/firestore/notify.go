package firestore

import (
	"context"

	gcf "cloud.google.com/go/firestore"
)

const (
	notifyCol      = "wf_notify"
	notifyTasksDoc = "tasks"
	notifyTermDoc  = "terminal"
)

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	out := make(chan struct{}, 1)
	hubCh, err := b.hub.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	go fanInStruct(ctx, out, hubCh)
	go b.watchTaskWake(ctx, out)
	return out, nil
}

func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	out := make(chan string, 1)
	hubCh, err := b.hub.SubscribeTerminal(ctx)
	if err != nil {
		return nil, err
	}
	go fanInString(ctx, out, hubCh)
	go b.watchTerminalWake(ctx, out)
	return out, nil
}

func (b *Backend) notifyTasks() {
	b.hub.NotifyTasks()
	b.touchTaskWake(context.Background())
}

func (b *Backend) notifyTerminal(instanceID string) {
	b.hub.NotifyTerminal(instanceID)
	b.touchTerminalWake(context.Background(), instanceID)
}

func (b *Backend) touchTaskWake(ctx context.Context) {
	_, _ = b.ref(notifyCol, notifyTasksDoc).Set(ctx, map[string]any{
		"n":  gcf.Increment(1),
		"at": gcf.ServerTimestamp,
	}, gcf.MergeAll)
}

func (b *Backend) touchTerminalWake(ctx context.Context, instanceID string) {
	_, _ = b.ref(notifyCol, notifyTermDoc).Set(ctx, map[string]any{
		"n":  gcf.Increment(1),
		"id": instanceID,
		"at": gcf.ServerTimestamp,
	}, gcf.MergeAll)
}

func (b *Backend) watchTaskWake(ctx context.Context, out chan<- struct{}) {
	it := b.ref(notifyCol, notifyTasksDoc).Snapshots(ctx)
	defer it.Stop()
	first := true
	for {
		snap, err := it.Next()
		if err != nil {
			return
		}
		if first {
			first = false
			continue // ignore initial
		}
		if !snap.Exists() {
			continue
		}
		select {
		case out <- struct{}{}:
		default:
		}
	}
}

func (b *Backend) watchTerminalWake(ctx context.Context, out chan<- string) {
	it := b.ref(notifyCol, notifyTermDoc).Snapshots(ctx)
	defer it.Stop()
	first := true
	for {
		snap, err := it.Next()
		if err != nil {
			return
		}
		if first {
			first = false
			continue
		}
		if !snap.Exists() {
			continue
		}
		id, _ := snap.Data()["id"].(string)
		if id == "" {
			continue
		}
		select {
		case out <- id:
		default:
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
