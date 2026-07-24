package workflow

import (
	"errors"
)

// ErrContinueAsNew is returned by ContinueAsNew and recognized by the worker.
var ErrContinueAsNew = errors.New("continue as new")

type continueAsNewError struct {
	Input []byte
}

func (e *continueAsNewError) Error() string { return ErrContinueAsNew.Error() }
func (e *continueAsNewError) Unwrap() error { return ErrContinueAsNew }

// ContinueAsNew ends the current run and starts a new one with the given input.
func ContinueAsNew[I any](ctx *Context, in I) error {
	payload, err := ctx.codec.Marshal(in)
	if err != nil {
		return err
	}
	return &continueAsNewError{Input: payload}
}

// AsContinueAsNew extracts continue-as-new input if err is ContinueAsNew.
func AsContinueAsNew(err error) (input []byte, ok bool) {
	var c *continueAsNewError
	if errors.As(err, &c) {
		return c.Input, true
	}
	return nil, false
}
