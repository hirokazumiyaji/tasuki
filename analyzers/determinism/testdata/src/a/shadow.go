package a

import (
	"time"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Local lookalikes of workflow escape hatches. They execute (or register)
// their callbacks without journaling, so closures passed to them must still
// be scanned: only callees resolving to the workflow package are exempt.

// SideEffect shadows workflow.SideEffect within this package.
func SideEffect(ctx *workflow.Context, fn func() string) (string, error) {
	return fn(), nil
}

// SetQueryHandler shadows workflow.SetQueryHandler within this package.
func SetQueryHandler[I, O any](ctx *workflow.Context, name string, fn func(I) (O, error)) {
}

func BadLocalSideEffect(ctx *workflow.Context, _ struct{}) error {
	_, _ = SideEffect(ctx, func() string {
		return time.Now().String() // want `time.Now is not allowed`
	})
	return nil
}

func BadLocalQueryHandler(ctx *workflow.Context, _ struct{}) error {
	SetQueryHandler(ctx, "q", func(v int) (int, error) {
		_ = time.Now() // want `time.Now is not allowed`
		return v, nil
	})
	return nil
}
