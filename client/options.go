package client

import "github.com/hirokazumiyaji/tasuki/codec"

type ClientOption func(*Client)

// WithCodec sets the codec used for inputs, signals, and results.
// It must match the codec configured on the workers.
func WithCodec(c codec.Codec) ClientOption {
	return func(cl *Client) { cl.codec = c }
}

type startOptions struct {
	id               string
	queue            string
	searchAttributes map[string]string
	memo             map[string]string
}

type StartOption func(*startOptions)

func WithID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

func WithQueue(queue string) StartOption {
	return func(o *startOptions) { o.queue = queue }
}

// WithSearchAttributes sets string key/value visibility metadata on Start.
// Values are exact-match filters for Client.List. Pass a copy; the map is not retained.
func WithSearchAttributes(attrs map[string]string) StartOption {
	return func(o *startOptions) {
		if len(attrs) == 0 {
			return
		}
		o.searchAttributes = make(map[string]string, len(attrs))
		for k, v := range attrs {
			if v == "" {
				continue
			}
			o.searchAttributes[k] = v
		}
	}
}

// WithMemo sets display-only string notes on Start (visible on Get, not List filters).
func WithMemo(attrs map[string]string) StartOption {
	return func(o *startOptions) {
		if len(attrs) == 0 {
			return
		}
		o.memo = make(map[string]string, len(attrs))
		for k, v := range attrs {
			if v == "" {
				continue
			}
			o.memo[k] = v
		}
	}
}

type signalOptions struct {
	dedupeID string
}

// SignalOption configures Client.Signal.
type SignalOption func(*signalOptions)

// WithDedupeID makes Signal idempotent for this instance: retries with the same
// ID do not deliver a second copy. Empty ID is ignored (same as omitting the option).
func WithDedupeID(id string) SignalOption {
	return func(o *signalOptions) { o.dedupeID = id }
}
