package dalgo2ingitdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"

	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// setupSpacesWithContactsSubcollection creates a top-level "spaces" collection
// and, under it, a "contacts" subcollection sharing the same schema. It returns
// the dal.DB and project root.
//
// The subcollection definition is produced by copying the generated "spaces"
// definition.yaml into spaces/.collection/subcollections/contacts/ — reusing the
// real writer guarantees the schema format matches what the loader expects.
func setupSpacesWithContactsSubcollection(t *testing.T) (dal.DB, string) {
	t.Helper()
	root := t.TempDir()
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	modifier := db.(ddl.SchemaModifier)
	col := dbschema.CollectionDef{
		Name: "spaces",
		Fields: []dbschema.FieldDef{
			{Name: "name", Type: dbschema.String, Nullable: false},
		},
	}
	if err := modifier.CreateCollection(context.Background(), col); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	registerRootCollection(t, root, "spaces", "spaces")

	spacesDef := filepath.Join(root, "spaces", ".collection", "definition.yaml")
	defBytes, err := os.ReadFile(spacesDef)
	if err != nil {
		t.Fatalf("read spaces definition.yaml: %v", err)
	}
	subDir := filepath.Join(root, "spaces", ".collection", "subcollections", "contacts")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", subDir, err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "definition.yaml"), defBytes, 0o644); err != nil {
		t.Fatalf("write contacts definition.yaml: %v", err)
	}
	return db, root
}

// TestNestedKeys_ScopedByParentRecord is the regression test for the per-space
// scoping fix (Option A). Two contact records share the same leaf collection
// ("contacts") and record id ("c1") but live under different parent spaces
// ("family" vs "work"). Before the fix both mapped to the same flat contacts
// dir + file and clobbered each other; after it, each is physically scoped under
// its parent (spaces/<spaceID>/contacts/c1.yaml) and round-trips independently.
func TestNestedKeys_ScopedByParentRecord(t *testing.T) {
	db, root := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()

	familyContact := dal.NewKeyWithParentAndID(dal.NewKeyWithID("spaces", "family"), "contacts", "c1")
	workContact := dal.NewKeyWithParentAndID(dal.NewKeyWithID("spaces", "work"), "contacts", "c1")

	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		if err := tx.Set(ctx, dal.NewRecordWithData(familyContact, map[string]any{"name": "Alice"})); err != nil {
			return err
		}
		return tx.Set(ctx, dal.NewRecordWithData(workContact, map[string]any{"name": "Bob"}))
	}); err != nil {
		t.Fatalf("write nested contacts: %v", err)
	}

	// Physical layout: nested under the parent space, no collision.
	familyPath := filepath.Join(root, "spaces", "family", "contacts", "$records", "c1.yaml")
	workPath := filepath.Join(root, "spaces", "work", "contacts", "$records", "c1.yaml")
	if _, err := os.Stat(familyPath); err != nil {
		t.Errorf("expected family contact file at %s: %v", familyPath, err)
	}
	if _, err := os.Stat(workPath); err != nil {
		t.Errorf("expected work contact file at %s: %v", workPath, err)
	}

	// Read each back and confirm they did NOT clobber each other.
	famRec := dal.NewRecordWithData(familyContact, map[string]any{})
	workRec := dal.NewRecordWithData(workContact, map[string]any{})
	if err := db.Get(ctx, famRec); err != nil {
		t.Fatalf("get family contact: %v", err)
	}
	if err := db.Get(ctx, workRec); err != nil {
		t.Fatalf("get work contact: %v", err)
	}
	if got := famRec.Data().(map[string]any)["name"]; got != "Alice" {
		t.Errorf("family contact name: got %v, want Alice", got)
	}
	if got := workRec.Data().(map[string]any)["name"]; got != "Bob" {
		t.Errorf("work contact name: got %v, want Bob", got)
	}
}
