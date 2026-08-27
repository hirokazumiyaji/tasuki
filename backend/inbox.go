package backend

import (
	"sort"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// InboxEntry is one inbox row as stored by a backend, kept for arrival-order
// sorting. Seq is a per-instance monotonic number allocated at append time;
// 0 marks legacy rows written before sequence tracking existed.
type InboxEntry struct {
	Seq       int64
	CreatedAt int64
	ID        int64
	Event     journal.Event
}

// SortInbox orders entries by arrival: legacy rows first by creation time,
// then by allocated sequence. CreatedAt units only need to be comparable
// within a single backend.
func SortInbox(entries []InboxEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Seq != b.Seq {
			return a.Seq < b.Seq
		}
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt < b.CreatedAt
		}
		return a.ID < b.ID
	})
}
