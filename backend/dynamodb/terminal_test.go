package dynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// deleteTasksForInstance queries instance_gsi and falls back to a Scan when
// the index is not queryable yet: absent on tables predating the index
// (Migrate backfills it), or still CREATING/backfilling right after the
// backfill. Genuine failures must propagate instead of silently degrading to
// a fleet-wide Scan on every terminal completion.
func TestIsMissingIndexError(t *testing.T) {
	missing := []error{
		errors.New("ValidationException: The table does not have the specified index: instance_gsi"),
		errors.New("ValidationException: no such index: instance_gsi"),
		errors.New("unknown index instance_gsi"),
		errors.New("ValidationException: The index is being created for table: instance_gsi"),
		errors.New("ValidationException: Index instance_gsi is being created and cannot be queried"),
		errors.New("ValidationException: Index instance_gsi is backfilling and not active"),
		&types.ResourceNotFoundException{Message: strPtr("Cannot do operations on a non-existent table or index: instance_gsi")},
	}
	for _, err := range missing {
		if !isMissingIndexError(err) {
			t.Errorf("isMissingIndexError(%v) = false, want true", err)
		}
	}
	real := []error{
		nil,
		errors.New("ProvisionedThroughputExceededException: throttled"),
		errors.New("ValidationException: One or more parameter values were invalid"),
		&types.ResourceNotFoundException{Message: strPtr("Cannot do operations on a non-existent table")},
	}
	for _, err := range real {
		if isMissingIndexError(err) {
			t.Errorf("isMissingIndexError(%v) = true, want false", err)
		}
	}
}

func strPtr(s string) *string { return &s }
