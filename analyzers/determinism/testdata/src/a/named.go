package a

import (
	"net"
	"net/http"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

type namedMap map[string]int
type namedChan chan int

// Defined (named) map and channel types carry *types.Named, not *types.Map
// or *types.Chan, so the range check must classify their underlying type.

func BadNamedMapRange(ctx *workflow.Context, m namedMap) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func BadNamedChanRange(ctx *workflow.Context, ch namedChan) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func OkNamedSliceRange(ctx *workflow.Context, s []int) error {
	for _, v := range s {
		_ = v
	}
	return nil
}

// Type conversions select a type name, not a function: net.IP(raw) and
// http.Header(values) must not be mistaken for net package I/O calls.

func OkNetIPConversion(ctx *workflow.Context, raw []byte) error {
	_ = net.IP(raw)
	return nil
}

func OkHTTPHeaderConversion(ctx *workflow.Context, vals map[string][]string) error {
	_ = http.Header(vals)
	return nil
}
