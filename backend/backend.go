package backend

import (
	"context"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Backend is the store contract used by Client and Worker.
type Backend interface {
	Capabilities() Capabilities
	CreateInstance(ctx context.Context, inst NewInstance) error
	GetInstance(ctx context.Context, id string) (*Instance, error)
	LoadWorkflow(ctx context.Context, instanceID string) (*WorkflowState, error)
	ClaimTasks(ctx context.Context, req ClaimRequest) ([]Task, error)
	CommitAdvancement(ctx context.Context, adv Advancement) error
	CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error
	FireDueTimers(ctx context.Context, limit int) (int, error)
}
