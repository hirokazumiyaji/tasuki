package backend

// FairTaskRef identifies a claimable task by id and owning instance; backends
// feed their FIFO-ordered candidates into FairPick.
type FairTaskRef struct {
	ID         int64
	InstanceID string
}

// FairOverfetch is the candidate page size backends should fetch when
// MaxPerInstance is set: wide enough to interleave several instances, bounded
// so each page's extra scan stays cheap. Fair claiming pages through
// candidates until the batch fills, so a victim further down the queue is
// still found.
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
//
// Backends that page candidates from the database should use FairPicker
// instead, so the batch can be filled from beyond a bounded page.
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

// FairPicker applies the FairPick policy incrementally over FIFO-ordered
// candidates. Backends feed each candidate through Offer while paging through
// the queue; paging stops once Full reports true, so a batch can be filled
// from arbitrarily deep in the queue without buffering the whole candidate
// pool in memory.
type FairPicker struct {
	limit       int
	perInstance int
	counts      map[string]int
	picked      []FairTaskRef
}

// NewFairPicker starts a fair selection of up to limit tasks with at most
// perInstance tasks per instance. perInstance must be positive; the uncapped
// path pages plain FIFO instead.
func NewFairPicker(limit, perInstance int) *FairPicker {
	return &FairPicker{
		limit:       limit,
		perInstance: perInstance,
		counts:      make(map[string]int),
		picked:      make([]FairTaskRef, 0, limit),
	}
}

// Offer feeds one candidate in FIFO order. It reports whether the batch is
// full after considering the candidate, so callers can stop paging early.
func (p *FairPicker) Offer(ref FairTaskRef) bool {
	if len(p.picked) >= p.limit {
		return true
	}
	if p.counts[ref.InstanceID] >= p.perInstance {
		return false
	}
	p.counts[ref.InstanceID]++
	p.picked = append(p.picked, ref)
	return len(p.picked) >= p.limit
}

// Full reports whether the batch has been filled.
func (p *FairPicker) Full() bool { return len(p.picked) >= p.limit }

// Release removes one previously picked candidate and frees its
// per-instance slot, so later queues can still fill the batch after a claim
// conflict. Backends call it when a selected candidate loses a concurrent
// claim race (the conditional update / transaction fails because another
// worker leased the task first, or the index returned a stale entry) and the
// candidate therefore occupies a slot without contributing to the batch. It
// reports whether a matching entry was removed; unknown IDs are a no-op.
func (p *FairPicker) Release(ref FairTaskRef) bool {
	for i, r := range p.picked {
		if r.ID != ref.ID {
			continue
		}
		p.picked = append(p.picked[:i], p.picked[i+1:]...)
		if p.counts[r.InstanceID] > 0 {
			p.counts[r.InstanceID]--
		}
		return true
	}
	return false
}

// Picked returns the fair selection gathered so far, in FIFO order.
func (p *FairPicker) Picked() []FairTaskRef { return p.picked }
