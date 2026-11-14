package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// The definition puts a parent record's subcollection instance next to the
// parent's record files: spaces keeps its records in $records (the default
// for a {key} name), so the contacts of space "family" live at
// spaces/$records/family/contacts/, the place `ingitdb validate` reads.
func TestDefinedLayout_SubCollectionBelowRecordsDir(t *testing.T) {
	t.Parallel()
	db, root := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()
	key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "family"), "contacts", "c1")
	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(key, map[string]any{"name": "Alice"}))
	}); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "spaces", "$records", "family", "contacts", "$records", "c1.yaml")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected the record at %s: %v", want, err)
	}
	if _, err := os.Stat(filepath.Join(root, "spaces", "family")); !os.IsNotExist(err) {
		t.Fatalf("nothing may be written at spaces/family, stat err = %v", err)
	}
}

// Records found where the definition does not put them (spaces/<key>/contacts/,
// the layout earlier driver versions wrote) make every read and write of that
// instance fail with the details, instead of a silent empty result.
func TestDefinedLayout_MislaidInstanceIsAnError(t *testing.T) {
	t.Parallel()
	db, root := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()
	mislaid := filepath.Join(root, "spaces", "family", "contacts", "$records")
	if err := os.MkdirAll(mislaid, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mislaid, "c1.yaml"), []byte("name: Alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "family"), "contacts", "c1")
	err := db.Get(ctx, record.NewRecordWithData(key, map[string]any{}))
	if !errors.Is(err, dalgo2ingitdb.ErrMislaidSubCollection) {
		t.Fatalf("Get: expected ErrMislaidSubCollection, got %v", err)
	}
	for _, want := range []string{
		"spaces/contacts", `parent record "family"`,
		filepath.Join("spaces", "family", "contacts") + " holds 1 file(s)",
		"but the definition puts them at ", filepath.Join("spaces", "$records", "family", "contacts") + " (spaces keeps",
		`records_dir "$records"`, `records_dir: "."`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	werr := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(key, map[string]any{"name": "Bob"}))
	})
	if !errors.Is(werr, dalgo2ingitdb.ErrMislaidSubCollection) {
		t.Fatalf("Set: expected ErrMislaidSubCollection, got %v", werr)
	}
	// Another parent without mislaid data is unaffected.
	other := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "work"), "contacts", "c1")
	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(other, map[string]any{"name": "Bob"}))
	}); err != nil {
		t.Fatalf("Set work contact: %v", err)
	}
}

// With records_dir "." on the parent the definition puts the instance at
// spaces/<key>/contacts/ (the Firestore-mirror layout); nothing is mislaid.
func TestDefinedLayout_RecordsDirDot(t *testing.T) {
	t.Parallel()
	_, root := setupSpacesWithContactsSubcollection(t)
	defPath := filepath.Join(root, "spaces", ".collection", "definition.yaml")
	b, err := os.ReadFile(defPath)
	if err != nil {
		t.Fatal(err)
	}
	s := strings.Replace(string(b), "record_file:\n", "record_file:\n    records_dir: .\n", 1)
	if s == string(b) {
		t.Fatalf("unexpected definition:\n%s", b)
	}
	if err := os.WriteFile(defPath, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "family"), "contacts", "c1")
	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(key, map[string]any{"name": "Alice"}))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "spaces", "family", "contacts", "$records", "c1.yaml")); err != nil {
		t.Fatalf("expected the record below spaces/family/contacts: %v", err)
	}
	if err := db.Get(ctx, record.NewRecordWithData(key, map[string]any{})); err != nil {
		t.Fatalf("Get: %v", err)
	}
}
