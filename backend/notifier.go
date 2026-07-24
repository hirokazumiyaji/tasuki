package backend

import "context"

// TaskNotifier is an optional Backend capability for wake hints.
// Workers may type-assert and wake early when tasks may be claimable.
type TaskNotifier interface {
	// Subscribe delivers coalesced wake hints. Cancelling ctx ends the subscription.
	// The returned error is only for subscribe/setup failure.
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}

// TerminalNotifier is an optional Backend capability for Result wake hints.
type TerminalNotifier interface {
	// SubscribeTerminal delivers instance IDs that may have become terminal.
	// Cancelling ctx ends the subscription.
	SubscribeTerminal(ctx context.Context) (<-chan string, error)
}
