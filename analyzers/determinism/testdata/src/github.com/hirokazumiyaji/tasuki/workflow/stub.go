package workflow

import "time"

type Context struct{}

func Now(ctx *Context) time.Time { return time.Time{} }

func SideEffect[T any](ctx *Context, fn func() T) (T, error) {
	var zero T
	return zero, nil
}

func NewUUID(ctx *Context) (string, error) { return "", nil }

func SetQueryHandler[I, O any](ctx *Context, name string, fn func(I) (O, error)) {}

func SetUpdateHandler[I, O any](ctx *Context, name string, fn func(*Context, I) (O, error)) {
}

func Sleep(ctx *Context, d time.Duration) error { return nil }
