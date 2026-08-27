package backend

// FairTaskRef identifies a claimable task by id and owning instance; backends
// feed their FIFO-ordered candidates into FairPick.
type FairTaskRef struct {
	ID         int64
	InstanceID string
}

// FairOverfetch is the candidate pool size backends should fetch when
// MaxPerInstance is set: wide enough to interleave several instances, bounded
// so the extra scan stays cheap.
func FairOverfetch(limit int) int {
	of := limit * 4
	if of < 64 {
		of = 64
	}
	if of > 4096 {
		of = 4096
	}
	if of < limit {
		of = limit
	}
	return of
}

// FairPick picks up to limit candidates with at most perInstance tasks per
// instance, preserving input (FIFO) order. perInstance <= 0 keeps plain FIFO
// (first limit candidates). Tasks of a flooding instance beyond the cap stay
// claimable on later polls.
func FairPick(refs []FairTaskRef, limit, perInstance int) []FairTaskRef {
	if limit <= 0 {
		return nil
	}
	if perInstance <= 0 {
		if len(refs) > limit {
			refs = refs[:limit]
		}
		return append([]FairTaskRef(nil), refs...)
	}
	counts := make(map[string]int)
	out := make([]FairTaskRef, 0, limit)
	for _, r := range refs {
		if len(out) == limit {
			break
		}
		if counts[r.InstanceID] >= perInstance {
			continue
		}
		counts[r.InstanceID]++
		out = append(out, r)
	}
	return out
}
