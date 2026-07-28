package workflow

import "errors"

var ErrCanceled = errors.New("workflow canceled")

// ErrLocalActivityRunnerMissing is returned when ExecuteLocal records a new
// command but no LocalActivityRunner was configured on the Context.
var ErrLocalActivityRunnerMissing = errors.New("local activity runner not configured")
