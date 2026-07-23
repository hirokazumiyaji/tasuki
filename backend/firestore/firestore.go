package firestore

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// Backend is the Cloud Firestore implementation of backend.Backend.
type Backend struct {
	client  *firestore.Client
	project string
}

// New opens a Firestore client. Set FIRESTORE_EMULATOR_HOST for the emulator.
func New(ctx context.Context, projectID string) (*Backend, error) {
	if projectID == "" {
		projectID = os.Getenv("TASUKI_FIRESTORE_PROJECT")
	}
	if projectID == "" {
		projectID = "tasuki"
	}
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("firestore: open: %w", err)
	}
	return &Backend{client: client, project: projectID}, nil
}

func (b *Backend) Close() error {
	return b.client.Close()
}

func (b *Backend) Client() *firestore.Client { return b.client }

// Migrate is a no-op for schemaless Firestore (indexes are documented separately).
func (b *Backend) Migrate(ctx context.Context) error {
	_ = ctx
	return nil
}

var collections = []string{
	"wf_instances",
	"wf_journal",
	"wf_inbox",
	"wf_tasks",
	"wf_timers",
	"wf_schedules",
}

// Reset deletes all documents in known collections (test helper).
func (b *Backend) Reset(ctx context.Context) error {
	for _, name := range collections {
		if err := deleteCollection(ctx, b.client, name); err != nil {
			return fmt.Errorf("firestore reset %s: %w", name, err)
		}
	}
	return nil
}

func deleteCollection(ctx context.Context, client *firestore.Client, name string) error {
	col := client.Collection(name)
	for {
		iter := col.Limit(100).Documents(ctx)
		numDeleted := 0
		batch := client.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return err
			}
			batch.Delete(doc.Ref)
			numDeleted++
		}
		iter.Stop()
		if numDeleted == 0 {
			return nil
		}
		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
	}
}
