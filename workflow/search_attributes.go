package workflow

import (
	"encoding/json"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// UpsertSearchAttributes merges attrs into the instance search attributes.
// Empty string values delete the corresponding key. The full merged map is
// recorded in the journal for deterministic replay.
func UpsertSearchAttributes(ctx *Context, attrs map[string]string) {
	recorded := ctx.recordedCommands()
	if ctx.cmdIndex < len(recorded) {
		ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeSearchAttributesUpdated}, nil)
		var merged map[string]string
		_ = json.Unmarshal(ev.Payload, &merged)
		ctx.searchAttributes = cloneSearchAttrs(merged)
		return
	}
	merged := mergeSearchAttrs(ctx.searchAttributes, attrs)
	payload, _ := json.Marshal(merged)
	if payload == nil {
		payload = []byte(`{}`)
	}
	ctx.recordOrReplay(journal.Command{Type: journal.TypeSearchAttributesUpdated}, payload)
	ctx.searchAttributes = merged
}

// SetSearchAttributes seeds the in-memory map (e.g. from Instance at Start).
// Used by the worker before running a workflow tick.
func (c *Context) SetSearchAttributes(attrs map[string]string) {
	c.searchAttributes = cloneSearchAttrs(attrs)
}

// SearchAttributes returns a copy of the current in-memory search attributes.
func (c *Context) SearchAttributes() map[string]string {
	return cloneSearchAttrs(c.searchAttributes)
}

func mergeSearchAttrs(base, patch map[string]string) map[string]string {
	out := cloneSearchAttrs(base)
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
		return map[string]string{}
	}
	return out
}

func cloneSearchAttrs(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
