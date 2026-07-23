package postgres

import (
	"context"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// Schedule APIs are implemented in M3 Task 4. Stubs keep the Backend interface satisfied.
func (b *Backend) UpsertSchedule(context.Context, backend.NewSchedule) error {
	return fmt.Errorf("postgres: schedules not implemented yet")
}

func (b *Backend) GetSchedule(context.Context, string) (*backend.Schedule, error) {
	return nil, fmt.Errorf("postgres: schedules not implemented yet")
}

func (b *Backend) PauseSchedule(context.Context, string, bool) error {
	return fmt.Errorf("postgres: schedules not implemented yet")
}

func (b *Backend) ClaimDueSchedules(context.Context, int) ([]backend.DueSchedule, error) {
	return nil, fmt.Errorf("postgres: schedules not implemented yet")
}
