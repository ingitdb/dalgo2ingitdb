package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
