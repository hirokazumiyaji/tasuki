package backend

import "github.com/hirokazumiyaji/tasuki/journal"

// HasMemoUpdate reports whether events include a memo_updated command.
func HasMemoUpdate(events []journal.Event) bool {
	for _, ev := range events {
		if ev.Type == journal.TypeMemoUpdated {
			return true
		}
	}
	return false
}

// LastMemoUpdate returns the merged map from the last memo_updated event, or nil.
func LastMemoUpdate(events []journal.Event) map[string]string {
	var last []byte
	for _, ev := range events {
		if ev.Type == journal.TypeMemoUpdated {
			last = ev.Payload
		}
	}
	if last == nil {
		return nil
	}
	m, err := SearchAttributesFromPayload(last)
	if err != nil {
		return nil
	}
	return m
}
