package spanner

import (
	"errors"
	"testing"
)

// purgeVictimRounds must delete on the first childless verification,
// re-sweep across rounds that observe raced rows, propagate sweep and
// verification errors immediately, and fail recoverably (parent intact,
// errPurgeWriteRace) when writers win every round — never by deleting the
// parent under leftover children (Codex round-19 P2 on #291: the old
// delete-then-second-sweep left orphans no retry could reselect).
func TestPurgeVictimRounds(t *testing.T) {
	t.Run("childless first round deletes once", func(t *testing.T) {
		sweeps := 0
		err := purgeVictimRounds(func() error { sweeps++; return nil },
			func() (bool, error) { return true, nil }, purgeChildlessVerifyRounds)
		if err != nil {
			t.Fatalf("rounds: %v", err)
		}
		if sweeps != 1 {
			t.Fatalf("sweeps = %d, want 1", sweeps)
		}
	})
	t.Run("raced rounds re-sweep then delete", func(t *testing.T) {
		sweeps := 0
		verifies := 0
		err := purgeVictimRounds(func() error { sweeps++; return nil },
			func() (bool, error) {
				verifies++
				return verifies >= 3, nil
			}, purgeChildlessVerifyRounds)
		if err != nil {
			t.Fatalf("rounds: %v", err)
		}
		if sweeps != 3 || verifies != 3 {
			t.Fatalf("sweeps = %d, verifies = %d, want 3 and 3", sweeps, verifies)
		}
	})
	t.Run("sweep error propagates without verifying", func(t *testing.T) {
		boom := errors.New("boom sweep")
		verifies := 0
		err := purgeVictimRounds(func() error { return boom },
			func() (bool, error) { verifies++; return true, nil }, purgeChildlessVerifyRounds)
		if !errors.Is(err, boom) {
			t.Fatalf("rounds = %v, want the sweep error", err)
		}
		if verifies != 0 {
			t.Fatalf("verifies = %d, want 0 (no delete after a failed sweep)", verifies)
		}
	})
	t.Run("verify error propagates without deleting", func(t *testing.T) {
		boom := errors.New("boom verify")
		sweeps := 0
		err := purgeVictimRounds(func() error { sweeps++; return nil },
			func() (bool, error) { return false, boom }, purgeChildlessVerifyRounds)
		if !errors.Is(err, boom) {
			t.Fatalf("rounds = %v, want the verify error", err)
		}
		if sweeps != 1 {
			t.Fatalf("sweeps = %d, want 1 (no retry on verification failure)", sweeps)
		}
	})
	t.Run("writers winning every round fail recoverably", func(t *testing.T) {
		sweeps := 0
		err := purgeVictimRounds(func() error { sweeps++; return nil },
			func() (bool, error) { return false, nil }, purgeChildlessVerifyRounds)
		if !errors.Is(err, errPurgeWriteRace) {
			t.Fatalf("rounds = %v, want errPurgeWriteRace (parent intact for retry)", err)
		}
		if sweeps != purgeChildlessVerifyRounds {
			t.Fatalf("sweeps = %d, want the full %d rounds before giving up", sweeps, purgeChildlessVerifyRounds)
		}
	})
}
