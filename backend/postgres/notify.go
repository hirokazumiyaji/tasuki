package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	taskNotifyChannel     = "tasuki_tasks"
	terminalNotifyChannel = "tasuki_terminal"
)

// notifyTasks best-effort wakes listeners. Errors are ignored.
func (b *Backend) notifyTasks(ctx context.Context) {
	_, _ = b.pool.Exec(ctx, `SELECT pg_notify($1, '')`, taskNotifyChannel)
}

// notifyTerminal best-effort wakes Result waiters for instanceID.
func (b *Backend) notifyTerminal(ctx context.Context, instanceID string) {
	_, _ = b.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, terminalNotifyChannel, instanceID)
}

// Subscribe implements backend.TaskNotifier using a dedicated LISTEN connection.
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	go b.listenLoop(ctx, taskNotifyChannel, func(string) {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	return ch, nil
}

// SubscribeTerminal implements backend.TerminalNotifier.
func (b *Backend) SubscribeTerminal(ctx context.Context) (<-chan string, error) {
	ch := make(chan string, 1)
	go b.listenLoop(ctx, terminalNotifyChannel, func(payload string) {
		select {
		case ch <- payload:
		default:
			// Drop if busy; waiter will poll on next wake or ticker.
		}
	})
	return ch, nil
}

func (b *Backend) listenLoop(ctx context.Context, channel string, onNotify func(payload string)) {
	backoff := 50 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		err := b.listenOnce(ctx, channel, onNotify)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 50 * time.Millisecond
	}
}

func (b *Backend) listenOnce(ctx context.Context, channel string, onNotify func(payload string)) error {
	cfg := b.pool.Config().ConnConfig.Copy()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		payload := ""
		if n != nil {
			payload = n.Payload
		}
		onNotify(payload)
	}
}
