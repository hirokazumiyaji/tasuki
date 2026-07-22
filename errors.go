package tasuki

import "errors"

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
