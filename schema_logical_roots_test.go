package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/config"
)

// assertLogicalCatalog exercises the public schema/query/keyed-read APIs for
// every discovered root, so discovery cannot advertise unusable physical IDs.
func assertLogicalCatalog(t *testing.T, root string, want []string) {
	t.Helper()
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	schemaReader, _ := dal.As[dbschema.SchemaReader](db)
	refs, err := schemaReader.ListCollections(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(refs))
	for i, ref := range refs {
		got[i] = ref.Name()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roots = %v, want %v", got, want)
	}
	for _, ref := range refs {
		schema, err := schemaReader.DescribeCollection(ctx, &ref)
		if err != nil {
			t.Fatal(err)
		}
		if schema.Name != ref.Name() || len(schema.Fields) == 0 {
			t.Fatalf("schema = %+v", schema)
		}
		query := dal.From(ref).NewQuery().SelectIntoRecord(func() record.Record {
			return record.NewRecordWithData(record.NewKeyWithID(ref.Name(), ""), map[string]any{})
		})
		reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		first, err := reader.Next()
		if err != nil {
			_ = reader.Close()
			t.Fatal(err)
		}
		keyed := record.NewRecordWithData(first.Key(), map[string]any{})
		if err := db.Get(ctx, keyed); err != nil {
			_ = reader.Close()
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first.Data(), keyed.Data()) {
			t.Fatalf("%s query/keyed data mismatch", ref.Name())
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogicalRoots_OriginalShippingLogs(t *testing.T) {
	// Copy to a temporary root: ordinary driver read locks must not modify testdata.
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS("testdata/shipping-logs")); err != nil {
		t.Fatal(err)
	}
	assertLogicalCatalog(t, root, []string{"baseline", "recovery11045example", "recovery11062example"})
}

func TestLogicalRoots_AliasesAndUnregisteredDirectories(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"identity", "custom/nested/physical", "unregistered"} {
		writeCollectionDef(t, root, path, countriesDef)
		if err := os.MkdirAll(filepath.Join(root, path, "$records"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path, "$records", "one.yaml"), []byte("name: One\npopulation: 1\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := config.WriteRootCollectionsToFile(root, map[string]string{"alias": "custom/nested/physical", "identity": "identity"}); err != nil {
		t.Fatal(err)
	}
	assertLogicalCatalog(t, root, []string{"alias", "identity"})
	db, _ := NewDatabase(root, newReader())
	schemaReader, _ := dal.As[dbschema.SchemaReader](db)
	for _, id := range []string{"unknown", "unregistered", "custom/nested/physical"} {
		ref := dal.NewRootCollectionRef(id, "")
		if _, err := schemaReader.DescribeCollection(context.Background(), &ref); !errors.Is(err, errCollectionNotInDefinition) {
			t.Fatalf("%s error = %v", id, err)
		}
	}
}

func TestLogicalRoots_LoadErrorsMatchTransactions(t *testing.T) {
	for _, content := range []string{"", "broken: ["} {
		t.Run(content, func(t *testing.T) {
			root := t.TempDir()
			if content != "" {
				if err := os.MkdirAll(filepath.Join(root, ".ingitdb"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, ".ingitdb/root-collections.yaml"), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			db, _ := NewDatabase(root, newReader())
			schemaReader, _ := dal.As[dbschema.SchemaReader](db)
			loadErr := db.RunReadonlyTransaction(context.Background(), func(context.Context, dal.ReadTransaction) error { return nil })
			_, listErr := schemaReader.ListCollections(context.Background(), nil)
			ref := dal.NewRootCollectionRef("unknown", "")
			_, describeErr := schemaReader.DescribeCollection(context.Background(), &ref)
			if loadErr != nil {
				if listErr == nil || describeErr == nil || listErr.Error() != loadErr.Error() || describeErr.Error() != loadErr.Error() {
					t.Fatalf("load=%v list=%v describe=%v", loadErr, listErr, describeErr)
				}
			} else if listErr != nil {
				t.Fatal(listErr)
			}
		})
	}
}

func TestLogicalRoots_InheritanceAndDataDirectory(t *testing.T) {
	root := t.TempDir()
	writeCollectionDef(t, root, ".collections/physical", "inherits: $base.yaml\ndata_dir: data\n")
	if err := os.Rename(filepath.Join(root, ".collections/physical/.collection/definition.yaml"), filepath.Join(root, ".collections/physical/definition.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".collections/physical/$base.yaml"), []byte(countriesDef), 0600); err != nil {
		t.Fatal(err)
	}
	if err := config.WriteRootCollectionsToFile(root, map[string]string{"alias": ".collections/physical"}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "data/$records"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data/$records/one.yaml"), []byte("name: One\npopulation: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	assertLogicalCatalog(t, root, []string{"alias"})
}

// Listing retains its historical ignored-parent behavior; describing a scoped
// reference uses the same schema resolver as the scoped record-read path.
func TestLogicalRoots_ScopedDescription(t *testing.T) {
	root := &ingitdb.CollectionDef{ID: "spaces", DirPath: t.TempDir(), SubCollections: map[string]*ingitdb.CollectionDef{
		"contacts": {ID: "contacts", Columns: map[string]*ingitdb.ColumnDef{"name": {Type: ingitdb.ColumnTypeString}}, ColumnsOrder: []string{"name"}},
	}}
	db := &Database{projectPath: t.TempDir(), reader: mockDefReader{def: &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"spaces": root}}}}
	parent := record.NewKeyWithID("spaces", "family")
	refs, err := db.ListCollections(context.Background(), parent)
	if err != nil || len(refs) != 1 || refs[0].Name() != "spaces" {
		t.Fatalf("roots=%v error=%v", refs, err)
	}
	ref := dal.NewCollectionRef("contacts", "", parent)
	got, err := db.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "contacts" || len(got.Fields) != 1 || got.Fields[0].Name != "name" {
		t.Fatalf("schema=%+v", got)
	}
}

func TestLogicalRoots_SchemaInsideWriteTransaction(t *testing.T) {
	root := t.TempDir()
	writeCollectionDef(t, root, "tags", countriesDef)
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	schema, _ := dal.As[dbschema.SchemaReader](db)
	err = db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, _ dal.ReadwriteTransaction) error {
		refs, err := schema.ListCollections(ctx, nil)
		if err != nil {
			return err
		}
		if len(refs) != 1 || refs[0].Name() != "tags" {
			t.Fatalf("roots=%v", refs)
		}
		_, err = schema.DescribeCollection(ctx, &refs[0])
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
