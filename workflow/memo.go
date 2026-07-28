package workflow

import (
	"encoding/json"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// UpsertMemo merges attrs into the instance memo.
// Empty string values delete the corresponding key. The full merged map is
// recorded in the journal for deterministic replay.
func UpsertMemo(ctx *Context, attrs map[string]string) {
	recorded := ctx.recordedCommands()
	if ctx.cmdIndex < len(recorded) {
		ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeMemoUpdated}, nil)
		var merged map[string]string
		_ = json.Unmarshal(ev.Payload, &merged)
		ctx.memo = cloneSearchAttrs(merged)
		return
	}
	merged := mergeSearchAttrs(ctx.memo, attrs)
	payload, _ := json.Marshal(merged)
	if payload == nil {
		payload = []byte(`{}`)
	}
	ctx.recordOrReplay(journal.Command{Type: journal.TypeMemoUpdated}, payload)
	ctx.memo = merged
}

// SetMemo seeds the in-memory memo (e.g. from Instance at Start).
func (c *Context) SetMemo(attrs map[string]string) {
	c.memo = cloneSearchAttrs(attrs)
}

// Memo returns a copy of the current in-memory memo.
func (c *Context) Memo() map[string]string {
	return cloneSearchAttrs(c.memo)
}
