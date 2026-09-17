package dalgo2ingitdb_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/config"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// snapshotFiles lists every regular file under root, relative and slash-separated.
func snapshotFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(root, path)
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	sort.Strings(files)
	return files
}

func setNamedRecord(ctx context.Context, db dal.DB, key *record.Key) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(key, map[string]any{"name": "x"}))
	})
}

// Traversal-shaped record IDs must never produce a file outside the
// collection's records directory.
func TestDB_RecordIDsCannotEscapeCollection(t *testing.T) {
	t.Parallel()
	db, root := setupSingleRecordDB(t)
	ctx := context.Background()

	contained := []string{
		"../../p2",
		"../secrets/$records/s1",
		".git/hooks/pre-commit",
		`..\..\p3`,
		"/abs",
	}
	for _, id := range contained {
		if err := setNamedRecord(ctx, db, record.NewKeyWithID("countries", id)); err != nil {
			t.Fatalf("Set %q: %v", id, err)
		}
	}
	for _, f := range snapshotFiles(t, root) {
		if strings.HasPrefix(f, ".ingitdb/") || strings.HasPrefix(f, "countries/.collection/") {
			continue
		}
		dir, name := filepath.Split(filepath.FromSlash(f))
		if filepath.ToSlash(dir) != "countries/$records/" || strings.ContainsAny(name, `/\`) {
			t.Errorf("file written outside countries/$records: %s", f)
		}
	}
	if keys := listCountryKeys(t, db); len(keys) != len(contained) {
		t.Errorf("listed keys %q, want %d records", keys, len(contained))
	}

	for _, id := range []string{"..", ".", "a\nb", "tab\there", "nul\x00"} {
		if err := setNamedRecord(ctx, db, record.NewKeyWithID("countries", id)); err == nil {
			t.Errorf("Set %q: want error, got nil", id)
		}
	}
}

// Parent record IDs in nested keys are path segments too.
func TestDB_ParentIDsCannotEscapeCollection(t *testing.T) {
	t.Parallel()
	db, root := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()

	for _, parentID := range []string{"..", ".", "x\ny"} {
		key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", parentID), "contacts", "c1")
		if err := setNamedRecord(ctx, db, key); err == nil {
			t.Errorf("Set under parent %q: want error, got nil", parentID)
		}
	}
	key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "../../outside"), "contacts", "c1")
	if err := setNamedRecord(ctx, db, key); err != nil {
		t.Fatalf("Set under parent with separators: %v", err)
	}
	want := filepath.Join(root, "spaces", "..%2F..%2Foutside", "contacts", "$records", "c1.yaml")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("want nested record at %s: %v", want, err)
	}
	got := record.NewRecordWithData(key, map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Errorf("Get under escaped parent: %v", err)
	}
}

func TestCreateCollection_RejectsControlCharactersAndBackslashes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	modifier, _ := dal.As[ddl.SchemaModifier](db)
	for _, name := range []string{"bad\nname", "tab\tname", "del\x7f", `a\b`} {
		col := dbschema.CollectionDef{Name: name, Fields: []dbschema.FieldDef{{Name: "name", Type: dbschema.String}}}
		if err := modifier.CreateCollection(context.Background(), col); err == nil {
			t.Errorf("CreateCollection %q: want error, got nil", name)
		}
	}
	if _, err := os.Stat(filepath.Join(root, config.IngitDBDirName, config.RootCollectionsFileName)); !os.IsNotExist(err) {
		t.Errorf("rejected names must not touch the registry: %v", err)
	}
}

// Collection names that are not plain YAML scalars must keep
// root-collections.yaml parseable.
func TestCreateCollection_RegistryQuotesYAMLSpecialNames(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	modifier, _ := dal.As[ddl.SchemaModifier](db)
	names := []string{"#notes", "plain", "- dash", "yes", "a'b"}
	for _, name := range names {
		col := dbschema.CollectionDef{Name: name, Fields: []dbschema.FieldDef{{Name: "name", Type: dbschema.String}}}
		if err := modifier.CreateCollection(context.Background(), col); err != nil {
			t.Fatalf("CreateCollection %q: %v", name, err)
		}
	}
	m, err := config.ReadRootCollectionsFromFile(root, ingitdb.NewReadOptions())
	if err != nil {
		t.Fatalf("root-collections.yaml unparseable: %v", err)
	}
	for _, name := range names {
		if m[name] != name {
			t.Errorf("registry[%q] = %q, want %q (registry %v)", name, m[name], name, m)
		}
	}
	if err := setNamedRecord(context.Background(), db, record.NewKeyWithID("#notes", "n1")); err != nil {
		t.Fatalf("Set into #notes: %v", err)
	}
}

// Safe names keep the exact bytes the previous plain-scalar writer produced.
func TestCreateCollection_RegistryBytesUnchangedForPlainNames(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	modifier, _ := dal.As[ddl.SchemaModifier](db)
	for _, name := range []string{"notes", "a_b", "Cities-2"} {
		col := dbschema.CollectionDef{Name: name, Fields: []dbschema.FieldDef{{Name: "name", Type: dbschema.String}}}
		if err := modifier.CreateCollection(context.Background(), col); err != nil {
			t.Fatalf("CreateCollection %q: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, config.IngitDBDirName, config.RootCollectionsFileName))
	if err != nil {
		t.Fatal(err)
	}
	if want := "Cities-2: Cities-2\na_b: a_b\nnotes: notes\n"; string(data) != want {
		t.Errorf("registry bytes:\n%q\nwant\n%q", data, want)
	}
	if err := modifier.DropCollection(context.Background(), "notes"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Cities-2", "a_b"} {
		if err := modifier.DropCollection(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	data, err = os.ReadFile(filepath.Join(root, config.IngitDBDirName, config.RootCollectionsFileName))
	if err != nil || len(data) != 0 {
		t.Errorf("empty registry: data %q err %v", data, err)
	}
}
