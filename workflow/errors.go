package workflow

import "errors"

var ErrCanceled = errors.New("workflow canceled")

// ErrLocalActivityRunnerMissing is returned when ExecuteLocal records a new
// command but no LocalActivityRunner was configured on the Context.
var ErrLocalActivityRunnerMissing = errors.New("local activity runner not configured")

// ErrDeadlineOutOfRange is returned by Sleep/SleepUntil when the
// UTC-normalized timer deadline falls outside the portable backend range
// (MySQL DATETIME(6): years 1000-9999). Recording it would fail every commit
// on MySQL, so the timer is rejected instead of recorded.
var ErrDeadlineOutOfRange = errors.New("workflow: sleep deadline outside portable UTC range (years 1000-9999)")
