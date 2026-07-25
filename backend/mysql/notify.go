package mysql

import "context"

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	return b.hub.Subscribe(ctx)
}

func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	return b.hub.SubscribeTerminal(ctx)
}

func (b *Backend) notifyTasks() { b.hub.NotifyTasks() }

func (b *Backend) notifyTerminal(instanceID string) { b.hub.NotifyTerminal(instanceID) }
