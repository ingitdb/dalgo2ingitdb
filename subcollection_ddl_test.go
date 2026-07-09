package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
)

// TestCreateCollection_SubCollection covers path-form names: the definition
// lands under the root's schema dir where the reader discovers it, records
// can be written/read at nested keys, and Delete on an unknown collection is
// an idempotent no-op.
func TestCreateCollection_SubCollection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	modifier := db.(ddl.SchemaModifier)
	ctx := context.Background()

	fields := []dbschema.FieldDef{{Name: "title", Type: dbschema.String, Nullable: true}}

	// Root must exist first.
	if err = modifier.CreateCollection(ctx, dbschema.CollectionDef{Name: "spaces/ext", Fields: fields}); err == nil {
		t.Fatal("expected error creating subcollection before its root collection")
	}
	if err = modifier.CreateCollection(ctx, dbschema.CollectionDef{Name: "spaces", Fields: fields}); err != nil {
		t.Fatal(err)
	}
	if err = modifier.CreateCollection(ctx, dbschema.CollectionDef{Name: "spaces/ext", Fields: fields}); err != nil {
		t.Fatal(err)
	}

	defPath := filepath.Join(root, "spaces", ".collection", "subcollections", "ext", "definition.yaml")
	if _, err = os.Stat(defPath); err != nil {
		t.Fatalf("subcollection definition.yaml not written: %v", err)
	}

	// Idempotent with IfNotExists; conflict without.
	if err = modifier.CreateCollection(ctx, dbschema.CollectionDef{Name: "spaces/ext", Fields: fields}, ddl.IfNotExists()); err != nil {
		t.Fatalf("IfNotExists should be a no-op, got: %v", err)
	}
	if err = modifier.CreateCollection(ctx, dbschema.CollectionDef{Name: "spaces/ext", Fields: fields}); err == nil {
		t.Fatal("expected already-exists error without IfNotExists")
	}

	// Write + read a record at a nested key.
	parent := dal.NewKeyWithID("spaces", "s1")
	nestedKey := dal.NewKeyWithParentAndID(parent, "ext", "contactus")
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		rec := dal.NewRecordWithData(nestedKey, map[string]any{"title": "hello"})
		rec.SetError(nil)
		return tx.Insert(ctx, rec)
	})
	if err != nil {
		t.Fatalf("insert at nested key: %v", err)
	}
	got := map[string]any{}
	rec := dal.NewRecordWithData(nestedKey, got)
	if err = db.Get(ctx, rec); err != nil {
		t.Fatalf("get nested record: %v", err)
	}
	if got["title"] != "hello" {
		t.Fatalf("nested record data = %v", got)
	}

	// Insert conflict surfaces ErrRecordAlreadyExists.
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		rec := dal.NewRecordWithData(nestedKey, map[string]any{"title": "again"})
		rec.SetError(nil)
		return tx.Insert(ctx, rec)
	})
	if !errors.Is(err, ErrRecordAlreadyExists) {
		t.Fatalf("expected ErrRecordAlreadyExists, got: %v", err)
	}

	// Delete on an unknown collection is an idempotent no-op.
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Delete(ctx, dal.NewKeyWithID("never_created", "x"))
	})
	if err != nil {
		t.Fatalf("delete on unknown collection should be a no-op, got: %v", err)
	}
}
