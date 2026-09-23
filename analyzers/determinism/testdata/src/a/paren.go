package a

import (
	"math/rand"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Parenthesized callees keep CallExpr.Fun as an ast.ParenExpr (preserved by
// gofmt); the analyzer must unwrap them before resolving the callee.

func BadParenSleep(ctx *workflow.Context, d time.Duration) error {
	(time.Sleep)(d) // want `time.Sleep is not allowed`
	return nil
}

func BadParenGetenv(ctx *workflow.Context, _ struct{}) error {
	_ = (os.Getenv)("HOME") // want `os.Getenv is not allowed`
	return nil
}

func BadParenRand(ctx *workflow.Context, _ struct{}) error {
	_ = (rand.Intn)(10) // want `math/rand`
	return nil
}

func BadParenMakeChan(ctx *workflow.Context, _ struct{}) error {
	_ = (make)(chan int) // want `make\(chan`
	return nil
}
