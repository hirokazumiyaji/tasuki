package backend

import "errors"

var (
	ErrAlreadyExists = errors.New("instance already exists")
	ErrConflict      = errors.New("optimistic lock conflict")
	ErrSuperseded    = errors.New("task superseded")
	ErrNotFound      = errors.New("not found")
)
