package hub

import (
	"context"
	"sync"
)

// Hub is an in-process coalesce fan-out for task and terminal wake hints.
type Hub struct {
	mu           sync.Mutex
	taskSubs     []*taskSub
	terminalSubs []*terminalSub
}

type taskSub struct {
	ch chan struct{}
}

type terminalSub struct {
	ch chan string
}

func New() *Hub {
	return &Hub{}
}

func (h *Hub) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	sub := &taskSub{ch: ch}
	h.mu.Lock()
	h.taskSubs = append(h.taskSubs, sub)
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.removeTaskSub(sub)
		// Do not close ch: a concurrent NotifyTasks may still hold a snapshot.
	}()
	return ch, nil
}

func (h *Hub) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 1)
	sub := &terminalSub{ch: ch}
	h.mu.Lock()
	h.terminalSubs = append(h.terminalSubs, sub)
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.removeTerminalSub(sub)
		// Do not close ch: a concurrent NotifyTerminal may still hold a snapshot.
	}()
	return ch, nil
}

func (h *Hub) removeTaskSub(sub *taskSub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.taskSubs[:0]
	for _, s := range h.taskSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	h.taskSubs = out
}

func (h *Hub) removeTerminalSub(sub *terminalSub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.terminalSubs[:0]
	for _, s := range h.terminalSubs {
		if s != sub {
			out = append(out, s)
		}
	}
	h.terminalSubs = out
}

// TerminalSubCount reports current terminal subscribers (test hook).
func (h *Hub) TerminalSubCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.terminalSubs)
}

// TaskSubCount reports current task subscribers (test hook).
func (h *Hub) TaskSubCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.taskSubs)
}

func (h *Hub) NotifyTasks() {
	h.mu.Lock()
	subs := append([]*taskSub(nil), h.taskSubs...)
	h.mu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
}

func (h *Hub) NotifyTerminal(instanceID string) {
	h.mu.Lock()
	subs := append([]*terminalSub(nil), h.terminalSubs...)
	h.mu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- instanceID:
		default:
		}
	}
}
