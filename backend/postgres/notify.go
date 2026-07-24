package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const taskNotifyChannel = "tasuki_tasks"

// notifyTasks best-effort wakes listeners. Errors are ignored.
func (b *Backend) notifyTasks(ctx context.Context) {
	_, _ = b.pool.Exec(ctx, `SELECT pg_notify($1, '')`, taskNotifyChannel)
}

// Subscribe implements backend.TaskNotifier using a dedicated LISTEN connection.
func (b *Backend) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)
	go b.listenLoop(ctx, ch)
	return ch, nil
}

func (b *Backend) listenLoop(ctx context.Context, ch chan struct{}) {
	backoff := 50 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		err := b.listenOnce(ctx, ch)
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

func (b *Backend) listenOnce(ctx context.Context, ch chan struct{}) error {
	cfg := b.pool.Config().ConnConfig.Copy()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+taskNotifyChannel); err != nil {
		return err
	}

	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
