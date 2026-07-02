package dalgo2ingitdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/dal"

	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// fixturesDir is the shared golden repo tree that pins the on-disk format (see
// FORMAT.md). The TS side (ingitdb-ts) keeps an identical copy and validates the
// same tree via @ingitdb/client-github, so writer (Go) and reader (TS) can't drift.
const fixturesDir = "testdata/format-fixtures"

// TestFormatFixtures_Read is the READER contract: opening the committed golden
// fixtures and reading the two nested contact records yields their exact field
// values. If the driver's read path stops understanding the documented layout,
// this fails.
func TestFormatFixtures_Read(t *testing.T) {
	t.Parallel()
	db, err := dalgo2ingitdb.NewDatabase(fixturesDir, validator.NewCollectionsReader())
	if err != nil {
		t.Fatalf("NewDatabase(fixtures): %v", err)
	}
	ctx := context.Background()

	cases := []struct{ space, wantName string }{
		{"family", "Alice"},
		{"work", "Bob"},
	}
	for _, c := range cases {
		key := dal.NewKeyWithParentAndID(dal.NewKeyWithID("spaces", c.space), "contacts", "c1")
		rec := dal.NewRecordWithData(key, map[string]any{})
		if err := db.Get(ctx, rec); err != nil {
			t.Fatalf("Get spaces/%s/contacts/c1: %v", c.space, err)
		}
		if got := rec.Data().(map[string]any)["name"]; got != c.wantName {
			t.Errorf("spaces/%s/contacts/c1 name: got %v, want %s", c.space, got, c.wantName)
		}
	}
}

// TestFormatFixtures_WriterMatches is the WRITER contract: the driver, writing the
// same records into a fresh project, produces record files byte-identical to the
// committed golden fixtures (same nested paths, same content). If the writer's
// layout or encoding changes, this fails — and the golden fixtures (and FORMAT.md,
// and the TS copy) must be updated deliberately in lockstep.
func TestFormatFixtures_WriterMatches(t *testing.T) {
	t.Parallel()
	db, root := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()

	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		fam := dal.NewKeyWithParentAndID(dal.NewKeyWithID("spaces", "family"), "contacts", "c1")
		if err := tx.Set(ctx, dal.NewRecordWithData(fam, map[string]any{"name": "Alice"})); err != nil {
			return err
		}
		work := dal.NewKeyWithParentAndID(dal.NewKeyWithID("spaces", "work"), "contacts", "c1")
		return tx.Set(ctx, dal.NewRecordWithData(work, map[string]any{"name": "Bob"}))
	}); err != nil {
		t.Fatalf("write records: %v", err)
	}

	// Compare each fixture record file against what the driver just wrote.
	relPaths := []string{
		filepath.Join("spaces", "family", "contacts", "$records", "c1.yaml"),
		filepath.Join("spaces", "work", "contacts", "$records", "c1.yaml"),
	}
	for _, rel := range relPaths {
		want, err := os.ReadFile(filepath.Join(fixturesDir, rel))
		if err != nil {
			t.Fatalf("read fixture %s: %v", rel, err)
		}
		got, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read written %s: %v", rel, err)
		}
		if string(got) != string(want) {
			t.Errorf("record %s: writer produced %q, golden fixture is %q", rel, string(got), string(want))
		}
	}
}
