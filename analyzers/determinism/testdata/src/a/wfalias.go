package a

import (
	"time"

	wf "github.com/hirokazumiyaji/tasuki/workflow"
)

func OkAliasSideEffect(ctx *wf.Context, _ struct{}) error {
	_, _ = wf.SideEffect(ctx, func() string {
		return time.Now().String()
	})
	return nil
}

func OkAliasQueryHandler(ctx *wf.Context, _ struct{}) error {
	wf.SetQueryHandler(ctx, "q", func(v int) (int, error) {
		_ = time.Now()
		return v, nil
	})
	return nil
}

func BadAliasUpdateHandler(ctx *wf.Context, _ struct{}) error {
	wf.SetUpdateHandler(ctx, "u", func(_ *wf.Context, v int) (int, error) {
		_ = time.Now() // want `time.Now is not allowed`
		return v, nil
	})
	return nil
}
