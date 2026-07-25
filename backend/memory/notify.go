package memory

import "context"

type taskSub struct {
	ch chan struct{}
}

type terminalSub struct {
	ch chan string
}

func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	sub := &taskSub{ch: ch}
	b.notifyMu.Lock()
	b.taskSubs = append(b.taskSubs, sub)
	b.notifyMu.Unlock()
	go func() {
		<-ctx.Done()
		b.removeTaskSub(sub)
		// Do not close ch: a concurrent notifyTasks may still hold a snapshot.
	}()
	return ch, nil
}

func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 1)
	sub := &terminalSub{ch: ch}
	b.notifyMu.Lock()
	b.terminalSubs = append(b.terminalSubs, sub)
	b.notifyMu.Unlock()
	go func() {
		<-ctx.Done()
		b.removeTerminalSub(sub)
		// Do not close ch: a concurrent notifyTerminal may still hold a snapshot.
	}()
	return ch, nil
}

func (b *Backend) removeTaskSub(sub *taskSub) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	out := b.taskSubs[:0]
	for _, s := range b.taskSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	b.taskSubs = out
}

func (b *Backend) removeTerminalSub(sub *terminalSub) {
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()
	out := b.terminalSubs[:0]
	for _, s := range b.terminalSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	b.terminalSubs = out
}

func (b *Backend) notifyTasks() {
	b.notifyMu.Lock()
	subs := append([]*taskSub(nil), b.taskSubs...)
	b.notifyMu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
}

func (b *Backend) notifyTerminal(instanceID string) {
	b.notifyMu.Lock()
	subs := append([]*terminalSub(nil), b.terminalSubs...)
	b.notifyMu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- instanceID:
		default:
		}
	}
}
