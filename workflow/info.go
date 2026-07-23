package workflow

import "time"

type WorkflowInfo struct {
	InstanceID string
	Name       string
	StartedAt  time.Time
}

func Info(ctx *Context) WorkflowInfo {
	return ctx.info
}
