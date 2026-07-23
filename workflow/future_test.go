package workflow_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestFuture_ImplementsAwaitable(t *testing.T) {
	var _ workflow.Awaitable = (*workflow.Future[string])(nil)
}
