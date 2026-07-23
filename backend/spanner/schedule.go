package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
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
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdateMap("wf_schedules", map[string]any{
				"id": s.ID, "cron": s.Cron, "workflow": s.Workflow, "queue": queue,
				"input": jsonVal(s.Input), "next_run_at": next, "paused": s.Paused,
				"created_at": now, "updated_at": now,
			}),
		})
	})
	return err
}

func (b *Backend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	row, err := b.client.Single().ReadRow(ctx, "wf_schedules", spanner.Key{id},
		[]string{"id", "cron", "workflow", "queue", "input", "next_run_at", "paused"})
	if err != nil {
		if isNotFound(err) {
			return nil, backend.ErrNotFound
		}
		return nil, err
	}
	var s backend.Schedule
	var input spanner.NullJSON
	if err := row.Columns(&s.ID, &s.Cron, &s.Workflow, &s.Queue, &input, &s.NextRunAt, &s.Paused); err != nil {
		return nil, err
	}
	s.Input = jsonBytes(input)
	return &s, nil
}

func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `UPDATE wf_schedules SET paused = @p, updated_at = @u WHERE id = @id`,
			Params: map[string]any{"p": paused, "u": nowUTC(), "id": id},
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return backend.ErrNotFound
		}
		return nil
	})
	return err
}

func (b *Backend) ClaimDueSchedules(ctx context.Context, limit int) ([]backend.DueSchedule, error) {
	if limit <= 0 {
		limit = 1
	}
	var out []backend.DueSchedule
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		out = nil
		now := nowUTC()
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT id, cron, workflow, queue, input, next_run_at, paused
				FROM wf_schedules
				WHERE paused = FALSE AND next_run_at <= @now
				ORDER BY next_run_at, id LIMIT @limit`,
			Params: map[string]any{"now": now, "limit": int64(limit)},
		})
		type rowSched struct {
			id, cron, workflow, queue string
			input                     spanner.NullJSON
			nextRunAt                 time.Time
			paused                    bool
		}
		var claimed []rowSched
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return err
			}
			var r rowSched
			if err := row.Columns(&r.id, &r.cron, &r.workflow, &r.queue, &r.input, &r.nextRunAt, &r.paused); err != nil {
				iter.Stop()
				return err
			}
			claimed = append(claimed, r)
		}
		iter.Stop()

		for _, r := range claimed {
			scheduledAt := r.nextRunAt.UTC()
			// Conditional advance of next_run_at (claim exclusivity).
			next, err := backend.NextCronTime(r.cron, scheduledAt)
			if err != nil {
				return err
			}
			n, err := txn.Update(ctx, spanner.Statement{
				SQL: `UPDATE wf_schedules SET next_run_at = @next, updated_at = @now
					WHERE id = @id AND next_run_at = @old AND paused = FALSE`,
				Params: map[string]any{
					"next": next, "now": now, "id": r.id, "old": r.nextRunAt,
				},
			})
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			instID := backend.ScheduleInstanceID(r.id, scheduledAt)
			inputBytes := jsonBytes(r.input)
			created := true
			err = txn.BufferWrite([]*spanner.Mutation{
				spanner.InsertMap("wf_instances", map[string]any{
					"id": instID, "name": r.workflow, "queue": r.queue, "status": "running",
					"input": jsonVal(inputBytes), "next_seq": int64(2),
					"created_at": now, "updated_at": now,
				}),
			})
			if isAlreadyExists(err) {
				created = false
				err = nil
			}
			if err != nil {
				return err
			}
			if created {
				if err := txn.BufferWrite([]*spanner.Mutation{
					spanner.InsertMap("wf_journal", map[string]any{
						"instance_id": instID, "seq": int64(1),
						"type": string(journal.TypeWorkflowStarted), "name": r.workflow,
						"payload": jsonVal(inputBytes), "recorded_at": now,
					}),
					spanner.InsertMap("wf_tasks", map[string]any{
						"id": newID(), "kind": "workflow", "queue": r.queue, "instance_id": instID,
						"attempt": int64(0), "visible_at": now, "created_at": now,
					}),
				}); err != nil {
					return err
				}
			}
			out = append(out, backend.DueSchedule{
				Schedule: backend.Schedule{
					ID: r.id, Cron: r.cron, Workflow: r.workflow, Queue: r.queue,
					Input: inputBytes, NextRunAt: next, Paused: r.paused,
				},
				InstanceID:  instID,
				ScheduledAt: scheduledAt,
				Created:     created,
			})
		}
		return nil
	})
	return out, err
}
