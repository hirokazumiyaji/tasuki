package memory_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backendtest"
)

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		b := memory.New()
		b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		return b
	})
}
