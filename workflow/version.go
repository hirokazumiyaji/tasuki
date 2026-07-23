package workflow

import (
	"encoding/json"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type versionPayload struct {
	ChangeID string `json:"change_id"`
	Version  int    `json:"version"`
}

// GetVersion records or returns a version marker for compatible code changes.
func GetVersion(ctx *Context, changeID string, min, max int) int {
	// Skip leading version markers that belong to other change IDs or unknown markers
	// when searching for our marker / recording position.
	for {
		rec, ok := ctx.peekCommand()
		if !ok {
			break
		}
		if rec.Type != journal.TypeVersionMarker {
			break
		}
		var p versionPayload
		_ = json.Unmarshal(rec.Payload, &p)
		if p.ChangeID == changeID {
			ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeVersionMarker, Name: changeID}, rec.Payload)
			_ = ev
			return p.Version
		}
		// Marker for a different change (or unknown): skip for old code that doesn't call GetVersion for it.
		ctx.skipCommand()
	}
	// No recorded marker for this change at this position → first visit records max, or min if
	// we're replaying past history without a marker (already consumed earlier commands).
	// If there are still recorded non-marker commands ahead, this is a "missed" marker position → min.
	if _, ok := ctx.peekCommand(); ok {
		return min
	}
	payload, _ := json.Marshal(versionPayload{ChangeID: changeID, Version: max})
	ctx.recordOrReplay(journal.Command{Type: journal.TypeVersionMarker, Name: changeID}, payload)
	return max
}
