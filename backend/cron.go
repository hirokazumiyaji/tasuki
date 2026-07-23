package backend

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ScheduleInstanceID builds the idempotent instance ID for a schedule fire.
func ScheduleInstanceID(scheduleID string, at time.Time) string {
	return scheduleID + ":" + at.UTC().Format(time.RFC3339)
}

// NextCronTime returns the next fire time after `from` for a standard 5-field cron.
func NextCronTime(expr string, from time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron: %w", err)
	}
	return sched.Next(from.UTC()), nil
}
