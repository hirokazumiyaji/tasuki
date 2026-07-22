package workflow

import "time"

type RetryPolicy struct {
	InitialInterval    time.Duration // default 1s
	BackoffCoefficient float64       // default 2.0
	MaxInterval        time.Duration // default 1m
	MaxAttempts        int           // default 0 (unlimited)
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.InitialInterval == 0 {
		p.InitialInterval = time.Second
	}
	if p.BackoffCoefficient == 0 {
		p.BackoffCoefficient = 2.0
	}
	if p.MaxInterval == 0 {
		p.MaxInterval = time.Minute
	}
	return p
}

type executeOptions struct {
	retry RetryPolicy
}

type ExecuteOption func(*executeOptions)

func WithRetry(p RetryPolicy) ExecuteOption {
	return func(o *executeOptions) { o.retry = p }
}

// Backoff returns the delay before the next attempt (attempt is 1-based, after a failure).
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	p = p.withDefaults()
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.InitialInterval)
	for i := 1; i < attempt; i++ {
		d *= p.BackoffCoefficient
		if time.Duration(d) >= p.MaxInterval {
			return p.MaxInterval
		}
	}
	out := time.Duration(d)
	if out > p.MaxInterval {
		return p.MaxInterval
	}
	return out
}
