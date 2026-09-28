package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestSeedFairAttemptedSkipsAcceptedInRequery is the regression
// test on #294: Limit=3/MaxPerInstance=2 over FIFO A1..A70,B1,C1 with
// A1..A69 locked. Pass 1 secures B1 WITHOUT overflowing, so B1 never enters
// the attempt log (picks are recorded only on overflow/requery passes); the
// overflow requery armed from the A69 snapshot then rescans A70,B1,C1.
//
// The fair picker is seeded only by per-instance COUNTS, not IDs, so without
// the seed it re-admits the already-secured B1 (B still has quota free) and
// fills [A70,B1] — the claiming update rejects the duplicate B1 while C1
// starves. With the seed the requery skips B1 and picks [A70,C1].
// Fail-without-fix: stub SeedFairAttempted to a no-op and the requery below
// fills [A70,B1], dropping C1.
func TestSeedFairAttemptedSkipsAcceptedInRequery(t *testing.T) {
	base := time.Now()
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Second)}
	}
	// Pass 1 secures B1 (id 71); A70 (id 70) and C1 (id 72) are still open.
	accepted := []backend.FairTaskRef{ref(71, "B")}
	// Attempt log as pass 1 leaves it: picks recorded only on overflow, and
	// pass 1 never overflowed, so B1 is absent. (Later drain passes record
	// their A-row picks; model a couple.)
	attempted := map[int64]struct{}{3: {}, 4: {}}

	// The requery rescan from the A69 snapshot, in FIFO order.
	rescan := []backend.FairTaskRef{ref(70, "A"), ref(71, "B"), ref(72, "C")}

	offer := func(seeded map[int64]struct{}) []backend.FairTaskRef {
		picker := backend.NewFairPicker(2, 2).TrackRejected()
		picker.Seed(accepted)
		for _, r := range rescan {
			// Mirror the mysql/postgres requery skip: requery passes skip
			// already-attempted IDs in favor of never-attempted dropped rows.
			if _, dup := seeded[r.ID]; dup {
				continue
			}
			if full := picker.Offer(r); full {
				break
			}
		}
		return picker.Picked()
	}

	// Without the seed the duplicate is admitted: [A70,B1], C1 starves.
	if got := offer(attempted); len(got) != 2 || got[0].ID != 70 || got[1].ID != 71 {
		t.Fatalf("unseeded requery picks %v, want [70 71] (the hole: B1 re-offered)", ids(got))
	}

	// Every accepted ID is skipped: [A70,C1].
	seeded := backend.SeedFairAttempted(attempted, accepted)
	if got := offer(seeded); len(got) != 2 || got[0].ID != 70 || got[1].ID != 72 {
		t.Fatalf("seeded requery picks %v, want [70 72] (B1 skipped, C1 reached)", ids(got))
	}
}

func TestSeedFairAttemptedCoversAllSecured(t *testing.T) {
	got := backend.SeedFairAttempted(nil, []backend.FairTaskRef{{ID: 1}, {ID: 2}, {ID: 2}})
	if len(got) != 2 {
		t.Fatalf("seeded %d IDs, want 2", len(got))
	}
	for _, id := range []int64{1, 2} {
		if _, ok := got[id]; !ok {
			t.Fatalf("secured ID %d missing from attempt set", id)
		}
	}
	// Seeding is additive and idempotent: existing entries survive, secured
	// IDs merge in.
	got[9] = struct{}{}
	got = backend.SeedFairAttempted(got, []backend.FairTaskRef{{ID: 1}, {ID: 7}})
	for _, id := range []int64{1, 2, 7, 9} {
		if _, ok := got[id]; !ok {
			t.Fatalf("ID %d missing after re-seed", id)
		}
	}
}

func ids(refs []backend.FairTaskRef) []int64 {
	out := make([]int64, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.ID)
	}
	return out
}
