package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// Keys carrying dots, quotes, backslashes, spaces, non-ASCII, or a leading
// `$` must filter exactly: the SQL-side predicate matches keys literally,
// never interpreting them as JSON paths.
func TestListInstancesSearchAttributesSpecialKeys(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "tasuki.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{
		"tenant.id": `a"b'c`,
		"sp ace":    "v",
		"café-☃":    "snowman",
		"plain":     "x",
		"$.tenant":  "dollar",
		"$":         "bare",
		`a"b\c`:     "quoted",
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sa-special", Name: "WF", Queue: "default", SearchAttributes: attrs,
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sa-other", Name: "WF", Queue: "default",
		SearchAttributes: map[string]string{"tenant.id": "elsewhere"},
	}); err != nil {
		t.Fatal(err)
	}
	// Decoy: a literal `tenant` key must not satisfy a `$.tenant` filter.
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: "sa-decoy", Name: "WF", Queue: "default",
		SearchAttributes: map[string]string{"tenant": "dollar"},
	}); err != nil {
		t.Fatal(err)
	}

	for key, val := range attrs {
		list, err := b.ListInstances(ctx, backend.InstanceFilter{
			SearchAttributes: map[string]string{key: val},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].ID != "sa-special" {
			t.Fatalf("filter %q=%q: got %+v, want [sa-special]", key, val, list)
		}
	}
	// AND over two special keys still narrows to the single row.
	and, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant.id": `a"b'c`, "café-☃": "snowman"},
		Limit:            10,
	})
	if err != nil || len(and) != 1 || and[0].ID != "sa-special" {
		t.Fatalf("AND special keys: %+v err=%v", and, err)
	}
	// A `$.tenant` filter must match the literal key only — not the `tenant`
	// key on the decoy row — and vice versa.
	dollar, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"$.tenant": "dollar"},
	})
	if err != nil || len(dollar) != 1 || dollar[0].ID != "sa-special" {
		t.Fatalf("$-prefixed key filter: %+v err=%v", dollar, err)
	}
	bare, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "dollar"},
	})
	if err != nil || len(bare) != 1 || bare[0].ID != "sa-decoy" {
		t.Fatalf("plain key filter must not match $-prefixed key: %+v err=%v", bare, err)
	}
	// Wrong value for a special key matches nothing (no path-injection row).
	miss, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant.id": `a"b'cX`},
	})
	if err != nil || len(miss) != 0 {
		t.Fatalf("want empty, got %+v err=%v", miss, err)
	}
	// Pagination composes with the special-key filter.
	paged, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"plain": "x"},
		Limit:            1,
		Offset:           1,
	})
	if err != nil || len(paged) != 0 {
		t.Fatalf("want empty second page, got %+v err=%v", paged, err)
	}
}
