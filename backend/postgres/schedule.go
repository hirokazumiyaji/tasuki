package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/jackc/pgx/v5"
)

func (b *Backend) UpsertSchedule(ctx context.Context, s backend.NewSchedule) error {
	if s.ID == "" || s.Cron == "" || s.Workflow == "" {
		return fmt.Errorf("schedule id, cron, and workflow are required")
	}
	queue := s.Queue
	if queue == "" {
		queue = "default"
	}
	var now time.Time
	if err := b.pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return err
	}
	next, err := backend.NextCronTime(s.Cron, now)
	if err != nil {
		return err
	}
	_, err = b.pool.Exec(ctx, `
		INSERT INTO wf_schedules (id, cron, workflow, queue, input, next_run_at, paused, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, now())
		ON CONFLICT (id) DO UPDATE SET
			cron = EXCLUDED.cron,
			workflow = EXCLUDED.workflow,
			queue = EXCLUDED.queue,
			input = EXCLUDED.input,
			next_run_at = EXCLUDED.next_run_at,
			paused = EXCLUDED.paused,
			updated_at = now()`,
		s.ID, s.Cron, s.Workflow, queue, jsonbOrNull(s.Input), next.UTC(), s.Paused)
	return err
}

func (b *Backend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	row := b.pool.QueryRow(ctx, `
		SELECT id, cron, workflow, queue, input, next_run_at, paused
		FROM wf_schedules WHERE id = $1`, id)
	var s backend.Schedule
	var input []byte
	if err := row.Scan(&s.ID, &s.Cron, &s.Workflow, &s.Queue, &input, &s.NextRunAt, &s.Paused); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	s.Input = input
	return &s, nil
}

func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	tag, err := b.pool.Exec(ctx, `
		UPDATE wf_schedules SET paused = $2, updated_at = now() WHERE id = $1`, id, paused)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) ClaimDueSchedules(ctx context.Context, limit int) ([]backend.DueSchedule, error) {
	if limit <= 0 {
		limit = 1
	}
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, cron, workflow, queue, input, next_run_at, paused
		FROM wf_schedules
		WHERE NOT paused AND next_run_at <= now()
		ORDER BY next_run_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	type rowSched struct {
		id, cron, workflow, queue string
		input                     []byte
		nextRunAt                 time.Time
		paused                    bool
	}
	var claimed []rowSched
	for rows.Next() {
		var r rowSched
		if err := rows.Scan(&r.id, &r.cron, &r.workflow, &r.queue, &r.input, &r.nextRunAt, &r.paused); err != nil {
			rows.Close()
			return nil, err
		}
		claimed = append(claimed, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return nil, tx.Commit(ctx)
	}

	out := make([]backend.DueSchedule, 0, len(claimed))
	for _, r := range claimed {
		scheduledAt := r.nextRunAt.UTC()
		instID := backend.ScheduleInstanceID(r.id, scheduledAt)
		var insertedID string
		err = tx.QueryRow(ctx, `
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq)
			VALUES ($1, $2, $3, 'running', $4::jsonb, 2)
			ON CONFLICT (id) DO NOTHING
			RETURNING id`,
			instID, r.workflow, r.queue, jsonbOrNull(r.input)).Scan(&insertedID)
		created := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if created {
			_, err = tx.Exec(ctx, `
				INSERT INTO wf_journal (instance_id, seq, type, name, payload)
				VALUES ($1, 1, $2, $3, $4::jsonb)`,
				instID, string(journal.TypeWorkflowStarted), r.workflow, jsonbOrNull(r.input))
			if err != nil {
				return nil, err
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO wf_tasks (kind, queue, instance_id, visible_at)
				VALUES ('workflow', $1, $2, now())`, r.queue, instID)
			if err != nil {
				return nil, err
			}
		}
		next, err := backend.NextCronTime(r.cron, scheduledAt)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `
			UPDATE wf_schedules SET next_run_at = $2, updated_at = now() WHERE id = $1`,
			r.id, next.UTC())
		if err != nil {
			return nil, err
		}
		out = append(out, backend.DueSchedule{
			Schedule: backend.Schedule{
				ID: r.id, Cron: r.cron, Workflow: r.workflow, Queue: r.queue,
				Input: append([]byte(nil), r.input...), NextRunAt: next, Paused: r.paused,
			},
			InstanceID:  instID,
			ScheduledAt: scheduledAt,
			Created:     created,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	for _, d := range out {
		if d.Created {
			b.notifyTasks(ctx)
			break
		}
	}
	return out, nil
}
