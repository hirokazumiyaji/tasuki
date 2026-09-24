package backend

import (
	"sort"
	"time"
)

// FairTaskRef identifies a claimable task by id and owning instance; backends
// feed their FIFO-ordered candidates into FairPick.
//
// VisibleAt is the candidate's FIFO ordering key, captured by the backend at
// scan time from the (visible_at, id) ordered candidate query (postgres,
// mysql). Refs that never need cross-pass reordering (single-pass backends,
// unit feeds) leave it zero; SortFairRefs only consumes keys set by such
// scans.
type FairTaskRef struct {
	ID         int64
	InstanceID string
	VisibleAt  time.Time
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

// MaxOverflowRequeryPasses is the hard backstop on successive
// overflow-requery segments one fair claim may issue (postgres, mysql). The
// primary stop condition is progress, not this count: each segment must newly
// attempt or secure a row (see the requery guards), so a claim over a flood
// that keeps advancing walks segment by segment until the batch fills, a
// segment proves exhaustion, or a segment makes no progress. The bound only
// caps pathological no-quiescence loops. Each segment resumes from the latest
// pre-overflow snapshot while skipping already-attempted IDs, so one segment
// advances past up to FairRejectedCap dropped rows; 64 segments cover ~128k
// flood rows (e.g. A18010 needs ~9 segments), far beyond any realistic single
// flood, while keeping worst-case extra scan work bounded at 64 additional
// bounded segment scans per claim. Claims still underfilled afterwards leave
// the rest to a later poll, which restarts from the head.
const MaxOverflowRequeryPasses = 64

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
// secured, the postgres/mysql refill loops re-issue bounded candidate
// requeries from the pre-overflow cursor position instead of returning
// underfilled, continuing through successive segments while each segment
// makes progress (newly attempted or secured rows), up to the
// MaxOverflowRequeryPasses backstop, so overflow-dropped rows get a second
// chance within the same claim; rows still dropped after that resurface on a
// later poll.
func (p *FairPicker) RejectedCapped() bool { return p.rejectedOverflow }

// SortFairRefs reorders refs into FIFO (scan) order by their captured
// (VisibleAt, ID) ordering keys, stable for ties. Refill paths (postgres,
// mysql) secure rows across passes: pass 1 may lock a later row (e.g. B1)
// while an earlier row (e.g. A2, rejected by the per-instance cap behind
// pick A1) is only secured on a refill pass after A1 is lost to a concurrent
// lock. Appending secured rows in lock order would return [B1 A2], violating
// the documented FIFO guarantee; sorting the secured batch by scan key
// restores [A2 B1].
//
// The key travels on the ref itself instead of a side rank map (issue #294
// round-12 P2): noting every scanned candidate's position in a map grows
// O(queue) and undoes the cap-bounding work, while only picked and
// retained/rejected-carry refs (bounded by limit+cap) can ever enter the
// returned batch. Every such ref already passed through the (visible_at, id)
// ordered scan, so its key is directly comparable with no retention at all:
// pending re-offers keep the key captured on their original scan pass, and a
// requery rescan re-keys from the same ordering. Zero keys (refs that never
// went through a keyed scan) sort before keyed ones; mixing the two never
// happens on the paths that call this.
func SortFairRefs(refs []FairTaskRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].VisibleAt.Equal(refs[j].VisibleAt) {
			return refs[i].ID < refs[j].ID
		}
		return refs[i].VisibleAt.Before(refs[j].VisibleAt)
	})
}

// FairSecuredCounts tallies secured refs per instance.
func FairSecuredCounts(secured []FairTaskRef) map[string]int {
	counts := make(map[string]int, len(secured))
	for _, r := range secured {
		counts[r.InstanceID]++
	}
	return counts
}

// TrimFairCarry bounds a retained refill carry to the rows that can actually
// be picked on the next pass (issue #294 round-17 P2): a plain visibility
// probe cannot see row locks, so probing the full carry retains every locked
// row and the picker then admits one retained row per pass (~2000 lock
// queries plus quadratic re-offers for a 2000-row locked carry). Only
// MaxPerInstance-minus-secured rows per instance can be picked — the picker
// rejects anything beyond that — and only Limit-minus-secured rows in total,
// so earlier rows beyond either quota can never contribute to this claim.
// Keep the FIFO-earliest rows within both quotas (by original scan order);
// locked extras stay dropped for later polls, which restart from the head.
// The result is O(limit), not O(carry).
func TrimFairCarry(pending, secured []FairTaskRef, limit, perInstance int) []FairTaskRef {
	if len(pending) == 0 || limit <= 0 || perInstance <= 0 {
		return pending
	}
	securedCounts := FairSecuredCounts(secured)
	remaining := limit - len(secured)
	if remaining <= 0 {
		return nil
	}
	sorted := append([]FairTaskRef(nil), pending...)
	SortFairRefs(sorted)
	kept := make([]FairTaskRef, 0, remaining)
	keptCounts := make(map[string]int)
	for _, r := range sorted {
		if len(kept) >= remaining {
			break
		}
		quota := perInstance - securedCounts[r.InstanceID]
		if quota <= 0 {
			continue
		}
		if keptCounts[r.InstanceID] >= quota {
			continue
		}
		keptCounts[r.InstanceID]++
		kept = append(kept, r)
	}
	return kept
}

// NoteFairLoss records per-instance outstanding lock/lease losses from one
// pass (issue #294 round-17 P2): picked refs that were not secured free quota
// a later pass can reuse. Instances whose secured count is back at the cap
// hold no outstanding quota — a refill already consumed the freed slot — so
// they resolve immediately (and previously recorded instances resolve once a
// later pass refills them to the cap).
func NoteFairLoss(outstanding map[string]struct{}, picked []FairTaskRef, securedIDs map[int64]bool, secured []FairTaskRef, perInstance int) {
	if outstanding == nil {
		return
	}
	for _, r := range picked {
		if !securedIDs[r.ID] {
			outstanding[r.InstanceID] = struct{}{}
		}
	}
	if perInstance <= 0 {
		return
	}
	counts := FairSecuredCounts(secured)
	for inst := range outstanding {
		if counts[inst] >= perInstance {
			delete(outstanding, inst)
		}
	}
}
