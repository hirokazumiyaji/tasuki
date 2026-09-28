package backend_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestTrimFairCarryWithResumeRecoversA68 is the regression test
// for the resume-cursor fix: Limit=2/MaxPerInstance=1 over A1..A68/B1 with
// A1..A67 locked. The earlier helper returned the first DROPPED row (A68)
// as the exclusive-requery cursor while retaining only through A67, so the
// requery (`>` A68) skipped A68 forever and every poll came back short while
// the head stayed locked. The fixed helper returns the last RETAINED row
// (A67): resuming strictly after it re-fetches A68 onward. This test drives
// the trim exactly as the postgres/mysql claim loops do — trim the carry,
// then keyset-requery exclusively after the cursor — and requires A68 to
// come back. With the old cursor (A68), the requery returns
// nothing and the test fails.
func TestTrimFairCarryWithResumeRecoversA68(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := func(id int64, inst string) backend.FairTaskRef {
		return backend.FairTaskRef{ID: id, InstanceID: inst, VisibleAt: base.Add(time.Duration(id) * time.Millisecond)}
	}
	var carry []backend.FairTaskRef
	for id := int64(2); id <= 68; id++ {
		carry = append(carry, ref(id, "A"))
	}
	secured := []backend.FairTaskRef{ref(1000, "B")}

	kept, resume, dropped := backend.TrimFairCarryWithResume(carry, secured, 2, 1)
	if !dropped {
		t.Fatal("dropped = false, want true (A68 remains past quota+margin+boundary)")
	}
	// The cursor must be a retained row: only then does the exclusive
	// requery below include the first unretained one.
	retained := make(map[int64]bool, len(kept))
	for _, r := range kept {
		retained[r.ID] = true
	}
	if !retained[resume.ID] {
		t.Fatalf("resume = A%d, want a retained row (exclusive requery would skip it)", resume.ID)
	}

	// Exclusive keyset requery over the carry, exactly as the claim loops
	// issue it: (visible_at, id) > (resume.visible_at, resume.id).
	var refetched []backend.FairTaskRef
	for _, r := range carry {
		if backend.FairRefBefore(resume, r) {
			refetched = append(refetched, r)
		}
	}
	if len(refetched) != 1 || refetched[0].ID != 68 {
		t.Fatalf("exclusive requery after resume A%d = %v, want [A68] (unlocked tail must recover)", resume.ID, refetched)
	}
}
