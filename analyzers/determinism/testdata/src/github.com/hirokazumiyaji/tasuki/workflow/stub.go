package workflow

import "time"

type Context struct{}

func Now(ctx *Context) time.Time { return time.Time{} }
