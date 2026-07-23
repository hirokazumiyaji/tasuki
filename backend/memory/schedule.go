package memory

import (
	"context"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

type schedule struct {
	id        string
	cron      string
	workflow  string
	queue     string
	input     []byte
	nextRunAt time.Time
	paused    bool
}

func (b *Backend) UpsertSchedule(_ context.Context, s backend.NewSchedule) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s.ID == "" || s.Cron == "" || s.Workflow == "" {
		return backend.ErrNotFound // reuse; better ErrInvalid — keep simple
	}
	queue := s.Queue
	if queue == "" {
		queue = "default"
	}
	next, err := backend.NextCronTime(s.Cron, b.now)
	if err != nil {
		return err
	}
	if existing, ok := b.schedules[s.ID]; ok {
		existing.cron = s.Cron
		existing.workflow = s.Workflow
		existing.queue = queue
		existing.input = append([]byte(nil), s.Input...)
		existing.paused = s.Paused
		existing.nextRunAt = next
		return nil
	}
	if b.schedules == nil {
		b.schedules = map[string]*schedule{}
	}
	b.schedules[s.ID] = &schedule{
		id: s.ID, cron: s.Cron, workflow: s.Workflow, queue: queue,
		input: append([]byte(nil), s.Input...), nextRunAt: next, paused: s.Paused,
	}
	return nil
}

func (b *Backend) GetSchedule(_ context.Context, id string) (*backend.Schedule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.schedules[id]
	if !ok {
		return nil, backend.ErrNotFound
	}
	return &backend.Schedule{
		ID: s.id, Cron: s.cron, Workflow: s.workflow, Queue: s.queue,
		Input: append([]byte(nil), s.input...), NextRunAt: s.nextRunAt, Paused: s.paused,
	}, nil
}

func (b *Backend) PauseSchedule(_ context.Context, id string, paused bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.schedules[id]
	if !ok {
		return backend.ErrNotFound
	}
	s.paused = paused
	return nil
}

func (b *Backend) ClaimDueSchedules(_ context.Context, limit int) ([]backend.DueSchedule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	var due []*schedule
	for _, s := range b.schedules {
		if s.paused || s.nextRunAt.After(b.now) {
			continue
		}
		due = append(due, s)
	}
	if len(due) == 0 {
		return nil, nil
	}
	// Stable order by id
	for i := 0; i < len(due); i++ {
		for j := i + 1; j < len(due); j++ {
			if due[j].id < due[i].id {
				due[i], due[j] = due[j], due[i]
			}
		}
	}
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]backend.DueSchedule, 0, len(due))
	for _, s := range due {
		scheduledAt := s.nextRunAt
		instID := backend.ScheduleInstanceID(s.id, scheduledAt)
		created := true
		err := b.createInstanceLocked(backend.NewInstance{
			ID: instID, Name: s.workflow, Queue: s.queue, Input: append([]byte(nil), s.input...),
		})
		if err != nil {
			if err == backend.ErrAlreadyExists {
				created = false
			} else {
				return nil, err
			}
		}
		next, err := backend.NextCronTime(s.cron, scheduledAt)
		if err != nil {
			return nil, err
		}
		s.nextRunAt = next
		out = append(out, backend.DueSchedule{
			Schedule: backend.Schedule{
				ID: s.id, Cron: s.cron, Workflow: s.workflow, Queue: s.queue,
				Input: append([]byte(nil), s.input...), NextRunAt: s.nextRunAt, Paused: s.paused,
			},
			InstanceID:  instID,
			ScheduledAt: scheduledAt,
			Created:     created,
		})
	}
	return out, nil
}
