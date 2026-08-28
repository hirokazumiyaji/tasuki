package backend

import (
	"fmt"
	"time"
)

// DefaultPurgeLimit is used by Backend.PurgeInstances when limit <= 0.
const DefaultPurgeLimit = 1000

// Terminal statuses eligible for purge. "continued" instances are finished
// (superseded by their Continue-As-New successor) and safe to remove as well.
var purgeEligibleStatuses = map[string]struct{}{
	"completed":  {},
	"failed":     {},
	"terminated": {},
	"canceled":   {},
	"continued":  {},
}

// DefaultPurgeStatuses is applied when nil or empty statuses are passed to
// Backend.PurgeInstances.
var DefaultPurgeStatuses = []string{"completed", "failed", "terminated", "canceled"}

// NormalizePurgeStatuses validates and expands the status filter for
// Backend.PurgeInstances. Unknown or non-terminal statuses are rejected so a
// typo cannot silently purge live workflows; running/stuck instances are never
// eligible for purge.
func NormalizePurgeStatuses(statuses []string) ([]string, error) {
	if len(statuses) == 0 {
		return append([]string(nil), DefaultPurgeStatuses...), nil
	}
	seen := make(map[string]struct{}, len(statuses))
	out := make([]string, 0, len(statuses))
	for _, s := range statuses {
		if _, ok := purgeEligibleStatuses[s]; !ok {
			return nil, fmt.Errorf("backend: status %q is not purge-eligible (terminal statuses only)", s)
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out, nil
}

// ValidatePurgeArgs checks olderThan/limit for Backend.PurgeInstances and
// applies the default limit. It returns the normalized statuses and limit.
func ValidatePurgeArgs(olderThan time.Duration, statuses []string, limit int) ([]string, int, error) {
	if olderThan < 0 {
		return nil, 0, fmt.Errorf("backend: olderThan must not be negative")
	}
	if limit <= 0 {
		limit = DefaultPurgeLimit
	}
	sts, err := NormalizePurgeStatuses(statuses)
	if err != nil {
		return nil, 0, err
	}
	return sts, limit, nil
}
