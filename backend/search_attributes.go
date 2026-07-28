package backend

import (
	"encoding/json"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// MergeSearchAttributes returns a copy of base with patch applied.
// Empty patch values delete the key. Nil base is treated as empty.
func MergeSearchAttributes(base, patch map[string]string) map[string]string {
	out := CloneSearchAttributes(base)
	if out == nil {
		out = map[string]string{}
	}
	for k, v := range patch {
		if v == "" {
			delete(out, k)
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// CloneSearchAttributes returns a shallow copy, or nil if m is empty.
func CloneSearchAttributes(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// MatchesSearchAttributes reports whether attrs contains every key/value in filter (AND).
// An empty filter always matches.
func MatchesSearchAttributes(attrs, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if attrs[k] != v {
			return false
		}
	}
	return true
}

// SearchAttributesFromPayload unmarshals a JSON object payload into a string map.
func SearchAttributesFromPayload(payload []byte) (map[string]string, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return CloneSearchAttributes(m), nil
}

// LastSearchAttributesUpdate returns the merged map from the last
// search_attributes_updated event in events, or nil if none.
func LastSearchAttributesUpdate(events []journal.Event) map[string]string {
	var last []byte
	for _, ev := range events {
		if ev.Type == journal.TypeSearchAttributesUpdated {
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
