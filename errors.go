package tasuki

import "errors"

var (
	ErrWorkflowNotRegistered = errors.New("workflow not registered")
	ErrActivityNotRegistered = errors.New("activity not registered")
	// ErrWorkerAlreadyRunning is returned by Worker.StartWithError when Start
	// is called on a worker that is already running.
	ErrWorkerAlreadyRunning = errors.New("tasuki: worker already running")
)

type nonRetryable struct{ err error }

func (n nonRetryable) Error() string { return n.err.Error() }
func (n nonRetryable) Unwrap() error { return n.err }

// NonRetryable marks an activity error as permanent (no retries).
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryable{err: err}
}

func IsNonRetryable(err error) bool {
	var n nonRetryable
	return errors.As(err, &n)
}
