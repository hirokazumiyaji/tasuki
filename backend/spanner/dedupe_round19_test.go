package spanner

import (
	"testing"

	"cloud.google.com/go/spanner"
)

// TestMatchDedupeRowVerifiesLegacyStoredKey covers the round-19 P1 fix on
// the Spanner side: like Firestore, the legacy branch compares the row's
// STORED key with the requested raw ID. Under composite (instance_id,
// dedupe_id) keys a framing collision cannot land a probe on a foreign row
// (readDedupeRow reads by (instanceID, key), so stored always equals the
// probed key), but the stored form keeps both backends on the identical rule
// — and a row whose stored key differs from the candidate (impossible in
// production, constructible in a test) must never match.
func TestMatchDedupeRowVerifiesLegacyStoredKey(t *testing.T) {
	legacy := spanner.NullInt64{}
	noOwner := spanner.NullString{}
	cases := []struct {
		name      string
		raw       string
		candidate string
		stored    string
		want      bool
	}{
		// The 3:3/3:x collision shape: a probe for "x" meeting a row stored
		// as "3:3:x" must not treat it as "x"'s guard (old code returned
		// true here: candidate "x" == requested "x").
		{"collision foreign stored key is not a match", "x", "x", "3:3:x", false},
		// Reverse direction: a probe for "3:3:x" meeting a row stored as
		// "x" must not suppress it either.
		{"reverse foreign stored key is not a match", "3:3:x", "3:3:x", "x", false},
		// Own verbatim guard still matches exactly.
		{"own legacy guard still matches", "x", "x", "x", true},
		{"own legacy guard still matches colon ID", "3:3:x", "3:3:x", "3:3:x", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchDedupeRow(tc.raw, tc.candidate, tc.stored, noOwner, legacy); got != tc.want {
				t.Fatalf("matchDedupeRow(%q, %q, %q) = %v, want %v",
					tc.raw, tc.candidate, tc.stored, got, tc.want)
			}
		})
	}
}
