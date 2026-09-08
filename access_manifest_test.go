package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

func TestNewDatabase_OwnerPolicyPersistsAndEnforcesReads(t *testing.T) {
	root := t.TempDir()
	legacy, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	modifier, ok := dal.As[ddl.SchemaModifier](legacy)
	if !ok {
		t.Fatal("legacy database lost schema modifier")
	}
	err = modifier.CreateCollection(context.Background(), dbschema.CollectionDef{Name: "countries", Fields: []dbschema.FieldDef{
		{Name: "name", Type: dbschema.String}, {Name: "population", Type: dbschema.Int},
	}})
	if err != nil {
		t.Fatal(err)
	}
	writeYAMLRecord(t, root, "countries", "france", "name: France\npopulation: 67000000\n")
	writeYAMLRecord(t, root, "countries", "japan", "name: Japan\npopulation: 125000000\n")

	accessDir := filepath.Join(root, ".ingitdb", "access")
	if err := os.MkdirAll(accessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "enabled: true\ndatabase: world-prod\npolicies: [readers.yaml]\n"
	policy := `apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: country-readers}
target: {database: world-prod}
composition: dalgo-hierarchical-v1
default: deny
scopes:
  - path: /countries
    rules:
      - id: own-row
        effect: allow
        operations: [query]
        fields: [name]
        where:
          op: "=="
          left: {field: name}
          right: {param: allowedName}
  - path: /countries/france
    rules:
      - id: france
        effect: allow
        operations: [get]
        fields: [name]
`
	if err := os.WriteFile(filepath.Join(accessDir, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(accessDir, "readers.yaml"), []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}

	open := func() dal.DB {
		db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
		if err != nil {
			t.Fatalf("NewDatabase with manifest: %v", err)
		}
		return db
	}
	db := open()
	if _, ok := dal.As[dbschema.SchemaReader](db); !ok {
		t.Error("secured database lost schema reader")
	}
	if _, ok := dal.As[ddl.SchemaModifier](db); ok {
		t.Error("secured database exposes schema mutation bypass")
	}
	rec := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatalf("allowed get: %v", err)
	}
	data := rec.Data().(map[string]any)
	if data["name"] != "France" || data["population"] != nil {
		t.Errorf("field policy result = %#v", data)
	}
	denied := record.NewRecordWithData(record.NewKeyWithID("countries", "japan"), map[string]any{})
	if err := db.Get(context.Background(), denied); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("record policy denial = %v", err)
	}
	otherQuery := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("cities", ""))).SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(context.Background(), otherQuery); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("collection policy denial = %v", err)
	}

	query := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("countries", ""))).SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(context.Background(), query); err == nil {
		t.Fatal("query without required context variable must fail closed")
	}
	ctx := access.WithVariables(context.Background(), map[string]any{"allowedName": "France"})
	reader, err := open().ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		t.Fatalf("allowed query after remount: %v", err)
	}
	got, err := reader.Next()
	if err != nil || got.Key().ID != "france" {
		t.Fatalf("filtered query first row = %v, %v", got, err)
	}
	if _, err := reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("filtered query has extra row: %v", err)
	}
}

func TestNewDatabase_RejectsSymlinkAccessManifest(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".ingitdb", "access")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "manifest-target.yaml")
	if err := os.WriteFile(target, []byte("enabled: false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "manifest.yaml")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader()); err == nil || db != nil {
		t.Fatalf("NewDatabase = (%v, %v), want nil, error", db, err)
	}
}

func TestNewDatabase_AccessManifestFailsClosed(t *testing.T) {
	tests := map[string]struct {
		manifest string
		policy   string
	}{
		"empty":                    {},
		"missing enabled":          {manifest: "{}\n"},
		"unknown field":            {manifest: "enabled: false\nextra: true\n"},
		"enabled without policies": {manifest: "enabled: true\ndatabase: x\n"},
		"missing policy":           {manifest: "enabled: true\ndatabase: x\npolicies: [missing.yaml]\n"},
		"invalid policy":           {manifest: "enabled: true\ndatabase: x\npolicies: [p.yaml]\n", policy: "not: a-policy\n"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, ".ingitdb", "access")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte(tc.manifest), 0o644); err != nil {
				t.Fatal(err)
			}
			if tc.policy != "" {
				if err := os.WriteFile(filepath.Join(dir, "p.yaml"), []byte(tc.policy), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader()); err == nil || db != nil {
				t.Fatalf("NewDatabase = (%v, %v), want nil, error", db, err)
			}
		})
	}
}

func TestOwnerPolicy_HidesDerivedColumnsAcrossReadPaths(t *testing.T) {
	_, root := setupFormulaDB(t)
	writePersonRecord(t, root, "ada", "first_name: Ada\nlast_name: Lovelace\nqty: 12\ndivisor: 4\n")
	writeOwnerPolicy(t, root, "people", "first_name", "last_name", "qty", "divisor", "full_name", "safe_ratio")
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	assertStoredOnly := func(t *testing.T, rec record.Record) {
		t.Helper()
		data := rec.Data().(map[string]any)
		if data["first_name"] != "Ada" {
			t.Fatalf("stored field missing: %#v", data)
		}
		if data["full_name"] != nil || data["safe_ratio"] != nil {
			t.Fatalf("computed field leaked: %#v", data)
		}
	}
	readPoint := func(ctx context.Context, session dal.ReadSession) {
		rec := record.NewRecordWithData(record.NewKeyWithID("people", "ada"), map[string]any{})
		if err := session.Get(ctx, rec); err != nil {
			t.Fatal(err)
		}
		assertStoredOnly(t, rec)
	}
	readQuery := func(ctx context.Context, session dal.ReadSession) {
		q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("people", ""))).SelectKeysOnly(reflect.String)
		r, err := session.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		assertStoredOnly(t, rec)
	}
	readPoint(context.Background(), db)
	readQuery(context.Background(), db)
	if err := db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		readPoint(ctx, tx)
		readQuery(ctx, tx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWithStoredOnlyReads_ProtectsOuterPolicyWithoutOwnerManifest(t *testing.T) {
	legacy, root := setupFormulaDB(t)
	writePersonRecord(t, root, "ada", "first_name: Ada\nlast_name: Lovelace\nqty: 12\ndivisor: 4\n")
	legacyRecord := record.NewRecordWithData(record.NewKeyWithID("people", "ada"), map[string]any{})
	if err := legacy.Get(context.Background(), legacyRecord); err != nil {
		t.Fatal(err)
	}
	if legacyRecord.Data().(map[string]any)["full_name"] != "Ada Lovelace" {
		t.Fatal("standalone legacy read lost computed columns")
	}

	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader(), dalgo2ingitdb.WithStoredOnlyReads())
	if err != nil {
		t.Fatal(err)
	}
	check := func(session dal.ReadSession) {
		rec := record.NewRecordWithData(record.NewKeyWithID("people", "ada"), map[string]any{})
		if err := session.Get(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
		data := rec.Data().(map[string]any)
		if data["first_name"] != "Ada" || data["full_name"] != nil {
			t.Fatalf("stored-only point read = %#v", data)
		}
		q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("people", ""))).SelectKeysOnly(reflect.String)
		r, err := session.ExecuteQueryToRecordsReader(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		row, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := row.Data().(map[string]any); got["first_name"] != "Ada" || got["full_name"] != nil {
			t.Fatalf("stored-only query = %#v", got)
		}
	}
	check(db)
	if err := db.RunReadonlyTransaction(context.Background(), func(_ context.Context, tx dal.ReadTransaction) error {
		check(tx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerPolicy_ComputedForeignKeyDoesNotReadOrLeakParent(t *testing.T) {
	_, root := setupComputedForeignKeyDB(t)
	writeYAMLRecord(t, root, "things", "thing-1", "owner_input: 7\n")
	// No users record exists. Protected reads must return the stored input
	// without evaluating or dereferencing the computed foreign key.
	writeOwnerPolicy(t, root, "things", "owner_input", "owner_key")
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	rec := record.NewRecordWithData(record.NewKeyWithID("things", "thing-1"), map[string]any{})
	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	data := rec.Data().(map[string]any)
	if data["owner_input"] != 7 {
		t.Fatalf("stored field = %#v", data)
	}
	if data["owner_key"] != nil {
		t.Fatalf("computed foreign key leaked: %#v", data)
	}
}

func TestOwnerPolicy_ReadsHonorCanceledContext(t *testing.T) {
	_, root := setupFormulaDB(t)
	writePersonRecord(t, root, "ada", "first_name: Ada\nlast_name: Lovelace\nqty: 12\ndivisor: 4\n")
	writeOwnerPolicy(t, root, "people", "first_name")
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("people", ""))).SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(ctx, q); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query = %v", err)
	}
	rec := record.NewRecordWithData(record.NewKeyWithID("people", "ada"), map[string]any{})
	if err := db.Get(ctx, rec); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled get = %v", err)
	}
}

func writeOwnerPolicy(t *testing.T, root, collection string, fields ...string) {
	t.Helper()
	dir := filepath.Join(root, ".ingitdb", "access")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "enabled: true\ndatabase: protected\npolicies: [owner.yaml]\n"
	policy := "apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: owner}\ntarget: {database: protected}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes:\n  - path: /" + collection + "\n    rules:\n      - {id: query, effect: allow, operations: [query], fields: [" + strings.Join(fields, ", ") + "]}\n  - path: /" + collection + "/*\n    rules:\n      - {id: get, effect: allow, operations: [get], fields: [" + strings.Join(fields, ", ") + "]}\n"
	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "owner.yaml"), []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestNewDatabase_AccessDirectoryRequiresManifest(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".ingitdb", "access"), 0o755); err != nil {
		t.Fatal(err)
	}
	if db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader()); err == nil || db != nil {
		t.Fatalf("NewDatabase = (%v, %v), want nil, error", db, err)
	}
}

func TestNewDatabase_RejectsSymlinkAccessDirectories(t *testing.T) {
	for _, ancestor := range []string{".ingitdb", "access"} {
		t.Run(ancestor, func(t *testing.T) {
			root := t.TempDir()
			target := t.TempDir()
			if ancestor == ".ingitdb" {
				if err := os.MkdirAll(filepath.Join(target, "access"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(root, ".ingitdb")); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			} else {
				if err := os.MkdirAll(filepath.Join(root, ".ingitdb"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(root, ".ingitdb", "access")); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			}
			if db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader()); err == nil || db != nil {
				t.Fatalf("NewDatabase = (%v, %v), want nil, error", db, err)
			}
		})
	}
}
