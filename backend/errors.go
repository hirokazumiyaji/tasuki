package backend

import "errors"

var (
	ErrAlreadyExists = errors.New("instance already exists")
	ErrConflict      = errors.New("optimistic lock conflict")
	ErrSuperseded    = errors.New("task superseded")
	ErrNotFound      = errors.New("not found")
	ErrBatchTooLarge = errors.New("batch too large")
)

// DefaultInboxBatchLimit is the max SignalBatch size for stores without write caps.
const DefaultInboxBatchLimit = 100

// InboxBatchLimit returns the max number of inbox items for one SendToInboxBatch.
func InboxBatchLimit(c Capabilities) int {
	if c.MaxAdvancementEffects > 0 {
		lim := c.MaxAdvancementEffects / 4
		if lim < 1 {
			lim = 1
		}
		return lim
	}
	return DefaultInboxBatchLimit
}
