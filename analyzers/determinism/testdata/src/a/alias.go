package a

import (
	osalias "os"
	timestd "time"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

func BadAliasTime(ctx *workflow.Context, _ struct{}) error {
	_ = timestd.Now() // want `time.Now is not allowed`
	return nil
}

func BadAliasSleep(ctx *workflow.Context, _ struct{}) error {
	_ = timestd.After(timestd.Second) // want `time.After is not allowed`
	return nil
}

func BadAliasOs(ctx *workflow.Context, _ struct{}) error {
	_ = osalias.Getenv("HOME") // want `os.Getenv is not allowed`
	return nil
}

func BadAliasOsArgs(ctx *workflow.Context, _ struct{}) error {
	_ = osalias.Args // want `os.Args is not allowed`
	return nil
}
