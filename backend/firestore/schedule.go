package firestore

import (
	"context"
	"fmt"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/api/iterator"
)

func (b *Backend) UpsertSchedule(ctx context.Context, s backend.NewSchedule) error {
	if s.ID == "" || s.Cron == "" || s.Workflow == "" {
		return fmt.Errorf("schedule id, cron, and workflow are required")
	}
	q := s.Queue
	if q == "" {
		q = "default"
	}
	now := nowUTC()
	next, err := backend.NextCronTime(s.Cron, now)
	if err != nil {
		return err
	}
	_, err = b.ref("wf_schedules", s.ID).Set(ctx, map[string]any{"id": s.ID, "cron": s.Cron, "workflow": s.Workflow, "queue": q, "input": jsonString(s.Input), "next_run_at": next, "paused": s.Paused, "created_at": now, "updated_at": now}, gcf.MergeAll)
	return err
}
func decodeSchedule(m map[string]any) *backend.Schedule {
	return &backend.Schedule{ID: str(m, "id"), Cron: str(m, "cron"), Workflow: str(m, "workflow"), Queue: str(m, "queue"), Input: bytes(m, "input"), NextRunAt: timestamp(m, "next_run_at"), Paused: boolean(m, "paused")}
}
func (b *Backend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	s, err := b.ref("wf_schedules", id).Get(ctx)
	if err != nil {
		return nil, err
	}
	if !s.Exists() {
		return nil, backend.ErrNotFound
	}
	return decodeSchedule(s.Data()), nil
}
func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	return b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		s, err := tx.Get(b.ref("wf_schedules", id))
		if err != nil {
			return err
		}
		if !s.Exists() {
			return backend.ErrNotFound
		}
		return tx.Update(s.Ref, []gcf.Update{{Path: "paused", Value: paused}, {Path: "updated_at", Value: nowUTC()}})
	})
}
func (b *Backend) ClaimDueSchedules(ctx context.Context, limit int) ([]backend.DueSchedule, error) {
	if limit <= 0 {
		limit = 1
	}
	now := nowUTC()
	it := b.col("wf_schedules").Where("paused", "==", false).Where("next_run_at", "<=", now).OrderBy("next_run_at", gcf.Asc).Limit(limit).Documents(ctx)
	defer it.Stop()
	out := []backend.DueSchedule{}
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return out, err
		}
		s := decodeSchedule(d.Data())
		scheduled := s.NextRunAt
		next, err := backend.NextCronTime(s.Cron, scheduled)
		if err != nil {
			return out, err
		}
		instanceID := backend.ScheduleInstanceID(s.ID, scheduled)
		created := false
		err = b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
			current, err := tx.Get(d.Ref)
			if err != nil {
				return err
			}
			if !current.Exists() || boolean(current.Data(), "paused") || !timestamp(current.Data(), "next_run_at").Equal(scheduled) {
				return backend.ErrConflict
			}
			existing, err := tx.Get(b.ref("wf_instances", instanceID))
			if err != nil {
				return err
			}
			if err = tx.Update(d.Ref, []gcf.Update{{Path: "next_run_at", Value: next}, {Path: "updated_at", Value: now}}); err != nil {
				return err
			}
			if existing.Exists() {
				return nil
			}
			inst := backend.NewInstance{ID: instanceID, Name: s.Workflow, Queue: s.Queue, Input: s.Input}
			if err = tx.Create(existing.Ref, instanceDoc(inst, s.Queue, now)); err != nil {
				return err
			}
			if err = tx.Create(b.ref("wf_journal", journalID(instanceID, 1)), journalDoc(instanceID, 1, journal.Event{Type: journal.TypeWorkflowStarted, Name: s.Workflow, Payload: s.Input}, now)); err != nil {
				return err
			}
			created = true
			return tx.Create(b.ref("wf_tasks", wfTaskID(instanceID)), workflowTaskDoc(instanceID, s.Queue, newID(), now))
		})
		if err == backend.ErrConflict {
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, backend.DueSchedule{Schedule: backend.Schedule{ID: s.ID, Cron: s.Cron, Workflow: s.Workflow, Queue: s.Queue, Input: s.Input, NextRunAt: next}, InstanceID: instanceID, ScheduledAt: scheduled, Created: created})
	}
	if len(out) > 0 {
		b.notifyTasks()
	}
	return out, nil
}
