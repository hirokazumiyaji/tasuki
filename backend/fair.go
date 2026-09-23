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
func FairOverfetch(limit int) int {	of := limit * 4
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

// FairRejectedCap bounds how many per-instance-cap rejections one FairPicker
// retains for Rejected (and, by extension, how many rows refill claimers
// carry across passes). Refill paths scan to the end of the ready queue, so
// an unbounded carry would retain O(queue) refs on a flooded instance;
// overflow beyond the cap is dropped and stays claimable for a later poll,
// which restarts its scan from the head. The cap bounds retention only, not
// the scan: callers keep paging past it (Offer drops further rejections) so
// a victim behind the flood is still reached in the same pass. Sized in the
// low thousands: large enough that ordinary refills never notice it, small
// enough that the carry stays cheap.
const FairRejectedCap = 2000

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
//
// Rejected retention is opt-in (TrackRejected) and off by default: callers
// that never refill (e.g. SQLite's single-pass scan) pay nothing, while
// refill paths (postgres, mysql) opt in to carry rejected rows forward.
type FairPicker struct {
	limit            int
	perInstance      int
	counts           map[string]int
	picked           []FairTaskRef
	rejected         []FairTaskRef
	rejectedOverflow bool
	trackRejected    bool
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

// TrackRejected opts into retaining per-instance-cap rejections for
// Rejected, and returns the picker for chaining. It must be called before
// offering candidates. Callers that refill after losing picked rows to
// concurrent locks (postgres, mysql) need this; single-pass callers leave it
// off so rejected backlog scanned past an over-quota flood never accumulates.
// Retention is capped at FairRejectedCap rows per pass, but the scan must
// continue past the cap: RejectedCapped is informational only, and stopping
// there would strand the unscanned tail behind a refill that re-rejects the
// retained rows (see Rejected).
func (p *FairPicker) TrackRejected() *FairPicker {
	p.trackRejected = true
	return p
}

// Offer feeds one candidate in FIFO order. It reports whether the batch is
// full after considering the candidate, so callers can stop paging early.
// Candidates rejected by the per-instance cap are retained for Rejected only
// when TrackRejected was opted into, so a refill pass can reconsider them if
// picked rows are later lost to concurrent lock contention. Retention stops
// at FairRejectedCap: further rejections are dropped (see RejectedCapped) so
// a flood-sized backlog never accumulates in memory. Dropping never stops the
// scan — callers keep offering until Full or the end of the queue, so a
// victim behind the flood is still picked in the same pass.
func (p *FairPicker) Offer(ref FairTaskRef) bool {
	if len(p.picked) >= p.limit {
		return true
	}
	if p.counts[ref.InstanceID] >= p.perInstance {
		if p.trackRejected {
			if len(p.rejected) < FairRejectedCap {
				p.rejected = append(p.rejected, ref)
			} else {
				p.rejectedOverflow = true
			}
		}
		return false
	}
	p.counts[ref.InstanceID]++
	p.picked = append(p.picked, ref)
	return len(p.picked) >= p.limit
}

// Full reports whether the batch has been filled.
func (p *FairPicker) Full() bool { return len(p.picked) >= p.limit }

// Seed records already-secured selections so a refill pass keeps the
// per-instance cap across rounds: a fresh picker for the remaining slots is
// seeded with the rows locked (postgres: claimed) by earlier passes, and the
// candidate scan continues past the dropped rows.
func (p *FairPicker) Seed(refs []FairTaskRef) {
	for _, r := range refs {
		p.counts[r.InstanceID]++
	}
}

// Picked returns the fair selection gathered so far, in FIFO order.
func (p *FairPicker) Picked() []FairTaskRef { return p.picked }

// Rejected returns the candidates skipped by the per-instance cap, in FIFO
// order. It is nil unless TrackRejected was opted into. Callers that refill
// after losing picked rows to concurrent locks must carry these forward: a
// rejected row can become eligible once the pick that blocked it is lost.
//
// Each entry is a compact FairTaskRef (ID plus owning instance ID; no
// payloads), retained only within one claim pass over the candidate scan and
// dropped with the per-pass picker afterwards. Retention is capped at
// FairRejectedCap rows per pass (see RejectedCapped): beyond that, rejections
// are dropped while the scan continues past them, so retention is O(cap)
// rather than O(queue). The cap bounds the carry, not the scan: stopping the
// scan at the cap would strand the unscanned tail, because the next refill
// re-offers the retained rows, fills its fresh carry to the cap with no
// picks, and exits empty while later polls restart at the head (victims past
// the flood starve, batches underfill). Dropped rows stay claimable for
// later polls.
func (p *FairPicker) Rejected() []FairTaskRef { return p.rejected }

// RejectedCapped reports whether rejected retention hit FairRejectedCap and
// began dropping rejections during this pass. It is informational only:
// callers keep paging (the scan, not the retention, reaches the victims past
// the flood); refill paths carry the retained rows forward bounded by the cap.
// When every retained row is then lost to concurrent locks with nothing
// secured, the postgres/mysql refill loops re-issue one bounded candidate
// requery from the pre-overflow cursor position instead of returning empty,
// so overflow-dropped rows get a second chance within the same claim; rows
// still dropped after that resurface on a later poll.
func (p *FairPicker) RejectedCapped() bool { return p.rejectedOverflow }
