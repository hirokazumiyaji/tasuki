package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func (b *Backend) UpsertSchedule(ctx context.Context, s backend.NewSchedule) error {
	if s.ID == "" || s.Cron == "" || s.Workflow == "" {
		return fmt.Errorf("schedule id, cron, and workflow are required")
	}
	queue := s.Queue
	if queue == "" {
		queue = "default"
	}
	now := nowUTC()
	next, err := backend.NextCronTime(s.Cron, now)
	if err != nil {
		return err
	}
	paused := 0
	if s.Paused {
		paused = 1
	}
	_, err = b.db.ExecContext(ctx, `
		INSERT INTO wf_schedules (id, cron, workflow, queue, input, next_run_at, paused, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			cron = excluded.cron,
			workflow = excluded.workflow,
			queue = excluded.queue,
			input = excluded.input,
			next_run_at = excluded.next_run_at,
			paused = excluded.paused,
			updated_at = excluded.updated_at`,
		s.ID, s.Cron, s.Workflow, queue, jsonOrNull(s.Input), formatTime(next),
		paused, formatTime(now), formatTime(now))
	return err
}

func (b *Backend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	row := b.db.QueryRowContext(ctx, `
		SELECT id, cron, workflow, queue, input, next_run_at, paused
		FROM wf_schedules WHERE id = ?`, id)
	var s backend.Schedule
	var input sql.NullString
	var nextRunStr string
	var paused int
	if err := row.Scan(&s.ID, &s.Cron, &s.Workflow, &s.Queue, &input, &nextRunStr, &paused); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	s.Input = scanJSONText(input)
	nextRun, err := parseTime(nextRunStr)
	if err != nil {
		return nil, err
	}
	s.NextRunAt = nextRun
	s.Paused = paused != 0
	return &s, nil
}

func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	p := 0
	if paused {
		p = 1
	}
	res, err := b.db.ExecContext(ctx, `
		UPDATE wf_schedules SET paused = ?, updated_at = ? WHERE id = ?`, p, nowStr(), id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return backend.ErrNotFound
	}
	return nil
}

func (b *Backend) ClaimDueSchedules(ctx context.Context, limit int) ([]backend.DueSchedule, error) {
	if limit <= 0 {
		limit = 1
	}
	conn, err := beginImmediate(ctx, b.db)
	if err != nil {
		return nil, err
	}
	defer rollbackConn(ctx, conn)

	now := nowStr()
	rows, err := conn.QueryContext(ctx, `
		SELECT id, cron, workflow, queue, input, next_run_at, paused
		FROM wf_schedules
		WHERE paused = 0 AND next_run_at <= ?
		ORDER BY next_run_at, id
		LIMIT ?`, now, limit)
	if err != nil {
		return nil, err
	}
	type rowSched struct {
		id, cron, workflow, queue string
		input                     sql.NullString
		nextRunAt                 time.Time
		paused                    int
	}
	var claimed []rowSched
	for rows.Next() {
		var r rowSched
		var nextStr string
		if err := rows.Scan(&r.id, &r.cron, &r.workflow, &r.queue, &r.input, &nextStr, &r.paused); err != nil {
			rows.Close()
			return nil, err
		}
		r.nextRunAt, err = parseTime(nextStr)
		if err != nil {
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
		return nil, commitConn(ctx, conn)
	}

	out := make([]backend.DueSchedule, 0, len(claimed))
	for _, r := range claimed {
		scheduledAt := r.nextRunAt.UTC()
		instID := backend.ScheduleInstanceID(r.id, scheduledAt)

		res, err := conn.ExecContext(ctx, `
			INSERT INTO wf_instances (id, name, queue, status, input, next_seq, created_at, updated_at)
			VALUES (?, ?, ?, 'running', ?, 2, ?, ?)`,
			instID, r.workflow, r.queue, jsonOrNull(scanJSONText(r.input)), nowStr(), nowStr())
		created := err == nil
		if err != nil {
			if !isUniqueViolation(err) {
				return nil, err
			}
			created = false
		} else if aff, _ := res.RowsAffected(); aff == 0 {
			created = false
		}

		if created {
			_, err = conn.ExecContext(ctx, `
				INSERT INTO wf_journal (instance_id, seq, type, name, payload, recorded_at)
				VALUES (?, 1, ?, ?, ?, ?)`,
				instID, string(journal.TypeWorkflowStarted), r.workflow, jsonOrNull(scanJSONText(r.input)), nowStr())
			if err != nil {
				return nil, err
			}
			_, err = conn.ExecContext(ctx, `
				INSERT INTO wf_tasks (kind, queue, instance_id, visible_at, created_at)
				VALUES ('workflow', ?, ?, ?, ?)`, r.queue, instID, nowStr(), nowStr())
			if err != nil {
				return nil, err
			}
		}

		next, err := backend.NextCronTime(r.cron, scheduledAt)
		if err != nil {
			return nil, err
		}
		_, err = conn.ExecContext(ctx, `
			UPDATE wf_schedules SET next_run_at = ?, updated_at = ? WHERE id = ?`,
			formatTime(next), nowStr(), r.id)
		if err != nil {
			return nil, err
		}
		out = append(out, backend.DueSchedule{
			Schedule: backend.Schedule{
				ID: r.id, Cron: r.cron, Workflow: r.workflow, Queue: r.queue,
				Input: scanJSONText(r.input), NextRunAt: next, Paused: r.paused != 0,
			},
			InstanceID:  instID,
			ScheduledAt: scheduledAt,
			Created:     created,
		})
	}
	if err := commitConn(ctx, conn); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		b.notifyTasks()
	}
	return out, nil
}
