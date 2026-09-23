package tasuki

import "errors"

var (
	ErrWorkflowNotRegistered = errors.New("workflow not registered")
	ErrActivityNotRegistered = errors.New("activity not registered")
	// ErrWorkerAlreadyRunning is returned by Worker.StartWithError when Start
	// is called on a worker that is already running.
	ErrWorkerAlreadyRunning = errors.New("tasuki: worker already running")
	// ErrWorkerShuttingDown is returned by Worker.StartWithError when Start
	// is called while a Shutdown is still in progress. Restarting in that
	// window would install a new execution context that the in-flight
	// Shutdown then cancels at grace expiry (while the old generation's
	// context leaks live), so the restart is rejected until Shutdown
	// returns.
	ErrWorkerShuttingDown = errors.New("tasuki: worker shutting down")
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
