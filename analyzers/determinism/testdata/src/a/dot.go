package a

import (
	. "time"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

func BadDotTime(ctx *workflow.Context, _ struct{}) error {
	_ = Now() // want `time.Now is not allowed`
	return nil
}

func BadDotSleep(ctx *workflow.Context, _ struct{}) error {
	Sleep(Second) // want `time.Sleep is not allowed`
	return nil
}
