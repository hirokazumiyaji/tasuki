package client

import "errors"

var (
	ErrAlreadyStarted = errors.New("workflow already started")
	ErrTerminated     = errors.New("workflow terminated")
	ErrFailed         = errors.New("workflow failed")
	ErrStuck          = errors.New("workflow stuck")
	ErrCanceled       = errors.New("workflow canceled")
)
