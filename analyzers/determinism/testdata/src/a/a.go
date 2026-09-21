package a

import (
	crand "crypto/rand"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
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

func BadSleep(ctx *workflow.Context, _ struct{}) error {
	time.Sleep(time.Second) // want `time.Sleep is not allowed`
	return nil
}

func BadAfter(ctx *workflow.Context, _ struct{}) error {
	_ = time.After(time.Second) // want `time.After is not allowed`
	return nil
}

func BadAfterFunc(ctx *workflow.Context, _ struct{}) error {
	_ = time.AfterFunc(time.Second, func() {}) // want `time.AfterFunc is not allowed`
	return nil
}

func BadNewTimer(ctx *workflow.Context, _ struct{}) error {
	_ = time.NewTimer(time.Second) // want `time.NewTimer is not allowed`
	return nil
}

func BadNewTicker(ctx *workflow.Context, _ struct{}) error {
	_ = time.NewTicker(time.Second) // want `time.NewTicker is not allowed`
	return nil
}

func BadTick(ctx *workflow.Context, _ struct{}) error {
	_ = time.Tick(time.Second) // want `time.Tick is not allowed`
	return nil
}

func BadUntil(ctx *workflow.Context, _ struct{}) error {
	_ = time.Until(time.Time{}) // want `time.Until is not allowed`
	return nil
}

func BadSelect(ctx *workflow.Context, _ struct{}) error {
	select {} // want `select statement is not allowed`
	return nil
}

func BadChanSend(ctx *workflow.Context, ch chan int) error {
	ch <- 1 // want `channel send is not allowed`
	return nil
}

func BadChanRecv(ctx *workflow.Context, ch chan int) error {
	_ = <-ch // want `channel receive is not allowed`
	return nil
}

func BadMakeChan(ctx *workflow.Context, _ struct{}) error {
	_ = make(chan int) // want `make\(chan`
	return nil
}

func BadMapRange(ctx *workflow.Context, _ struct{}) error {
	for k := range map[string]int{"a": 1} { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func OkSliceRange(ctx *workflow.Context, _ struct{}) error {
	for _, v := range []int{1, 2} {
		_ = v
	}
	return nil
}

func BadOsGetenv(ctx *workflow.Context, _ struct{}) error {
	_ = os.Getenv("HOME") // want `os.Getenv is not allowed`
	return nil
}

func BadOsLookupEnv(ctx *workflow.Context, _ struct{}) error {
	_, _ = os.LookupEnv("HOME") // want `os.LookupEnv is not allowed`
	return nil
}

func BadOsEnviron(ctx *workflow.Context, _ struct{}) error {
	_ = os.Environ() // want `os.Environ is not allowed`
	return nil
}

func BadOsHostname(ctx *workflow.Context, _ struct{}) error {
	_, _ = os.Hostname() // want `os.Hostname is not allowed`
	return nil
}

func BadOsGetpid(ctx *workflow.Context, _ struct{}) error {
	_ = os.Getpid() // want `os.Getpid is not allowed`
	return nil
}

func BadOsArgs(ctx *workflow.Context, _ struct{}) error {
	_ = os.Args // want `os.Args is not allowed`
	return nil
}

func BadSync(ctx *workflow.Context, _ struct{}) error {
	var mu sync.Mutex
	mu.Lock() // want `sync.Lock is not allowed`
	return nil
}

func BadAtomic(ctx *workflow.Context, _ struct{}) error {
	var x int64
	_ = atomic.AddInt64(&x, 1) // want `sync/atomic`
	return nil
}

func BadRuntime(ctx *workflow.Context, _ struct{}) error {
	_ = runtime.NumCPU() // want `runtime.NumCPU is not allowed`
	return nil
}

func BadNetDial(ctx *workflow.Context, _ struct{}) error {
	_, _ = net.Dial("tcp", "example.com:80") // want `net.Dial is not allowed`
	return nil
}

func BadHTTP(ctx *workflow.Context, _ struct{}) error {
	_, _ = http.Get("http://example.com") // want `net/http.Get is not allowed`
	return nil
}

func BadExec(ctx *workflow.Context, _ struct{}) error {
	_ = exec.Command("echo") // want `os/exec.Command is not allowed`
	return nil
}

func Ok(ctx *workflow.Context, _ struct{}) error {
	_ = workflow.Now(ctx)
	return nil
}

func OkSideEffect(ctx *workflow.Context, _ struct{}) error {
	_, _ = workflow.SideEffect(ctx, func() string {
		return time.Now().String()
	})
	return nil
}

func OkSideEffectRand(ctx *workflow.Context, _ struct{}) error {
	_, _ = workflow.SideEffect(ctx, func() int {
		return rand.Intn(10)
	})
	return nil
}

func OkQueryHandler(ctx *workflow.Context, _ struct{}) error {
	workflow.SetQueryHandler(ctx, "q", func(v int) (int, error) {
		_ = time.Now()
		return v, nil
	})
	return nil
}

func BadUpdateHandler(ctx *workflow.Context, _ struct{}) error {
	workflow.SetUpdateHandler(ctx, "u", func(_ *workflow.Context, v int) (int, error) {
		_ = time.Now() // want `time.Now is not allowed`
		return v, nil
	})
	return nil
}

// Non-workflow functions must not be flagged (covers isWorkflowFunc false paths).
func NotWorkflow() {
	_ = time.Now()
}

func AlsoNotWorkflow(x int) error {
	go func() {}()
	return nil
}
