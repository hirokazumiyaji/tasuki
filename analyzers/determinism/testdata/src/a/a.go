package a

import (
	crand "crypto/rand"
	"math/rand"
	"time"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

func BadTime(ctx *workflow.Context, _ struct{}) error {
	_ = time.Now() // want `time.Now is not allowed`
	return nil
}

func BadSince(ctx *workflow.Context, _ struct{}) error {
	_ = time.Since(time.Time{}) // want `time.Since is not allowed`
	return nil
}

func BadGo(ctx *workflow.Context, _ struct{}) error {
	go func() {}() // want `go statement is not allowed`
	return nil
}

func BadMathRand(ctx *workflow.Context, _ struct{}) error {
	_ = rand.Intn(10) // want `math/rand`
	return nil
}

func BadCryptoRand(ctx *workflow.Context, _ struct{}) error {
	var b [1]byte
	_, _ = crand.Read(b[:]) // want `crypto/rand`
	return nil
}

func Ok(ctx *workflow.Context, _ struct{}) error {
	_ = workflow.Now(ctx)
	return nil
}
