package dalgo2ingitdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"gopkg.in/yaml.v3"
)

func describeSourceCase(t *testing.T, mutate func(*ingitdb.CollectionDef)) (*dbschema.CollectionDef, error) {
	t.Helper()
	root := t.TempDir()
	def := &ingitdb.CollectionDef{ID: "items", Columns: map[string]*ingitdb.ColumnDef{"x": {Type: ingitdb.ColumnTypeString}}, ColumnsOrder: []string{"x"}, SourceSchema: &ingitdb.SourceSchemaDef{KeyMode: "source-primary-key", Fields: []ingitdb.SourceFieldDef{{Name: "x", Type: "string"}}}}
	mutate(def)
	path := filepath.Join(root, "items", ".collection", "definition.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	ref := dal.NewRootCollectionRef("items", "")
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("schema reader unavailable")
	}
	return reader.DescribeCollection(context.Background(), &ref)
}

func TestDescribeSourceSchemaRejectsCorruptMetadata(t *testing.T) {
	cases := []struct {
		name, want string
		mutate     func(*ingitdb.CollectionDef)
	}{
		{"field count", "do not match columns", func(d *ingitdb.CollectionDef) { d.SourceSchema.Fields = nil }},
		{"field missing", "missing from columns", func(d *ingitdb.CollectionDef) { d.SourceSchema.Fields[0].Name = "other" }},
		{"field order", "out of order", func(d *ingitdb.CollectionDef) {
			d.Columns["y"] = &ingitdb.ColumnDef{Type: ingitdb.ColumnTypeString}
			d.ColumnsOrder = []string{"y", "x"}
			d.SourceSchema.Fields = []ingitdb.SourceFieldDef{{Name: "x", Type: "string"}, {Name: "y", Type: "string"}}
		}},
		{"source type", "unsupported source type", func(d *ingitdb.CollectionDef) { d.SourceSchema.Fields[0].Type = "alien" }},
		{"default JSON", "default for", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "string"
			d.SourceSchema.Fields[0].DefaultJSON = "{"
		}},
		{"default trailing", "trailing content", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "string"
			d.SourceSchema.Fields[0].DefaultJSON = `"a" "b"`
		}},
		{"byte default", "byte default", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "[]uint8"
			d.SourceSchema.Fields[0].DefaultJSON = "42"
		}},
		{"int shape", "not numeric", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "int"
			d.SourceSchema.Fields[0].DefaultJSON = `"a"`
		}},
		{"int overflow", "value out of range", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "int64"
			d.SourceSchema.Fields[0].DefaultJSON = "9223372036854775808"
		}},
		{"nil shape", "has value", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "<nil>"
			d.SourceSchema.Fields[0].DefaultJSON = "1"
		}},
		{"string shape", "not a string", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "string"
			d.SourceSchema.Fields[0].DefaultJSON = "1"
		}},
		{"bool shape", "not a bool", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "bool"
			d.SourceSchema.Fields[0].DefaultJSON = "1"
		}},
		{"float shape", "not numeric", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "float64"
			d.SourceSchema.Fields[0].DefaultJSON = `"a"`
		}},
		{"float overflow", "value out of range", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "float64"
			d.SourceSchema.Fields[0].DefaultJSON = "1e9999"
		}},
		{"unknown literal", "unknown default literal", func(d *ingitdb.CollectionDef) {
			d.SourceSchema.Fields[0].DefaultKind = "literal"
			d.SourceSchema.Fields[0].DefaultType = "alien"
			d.SourceSchema.Fields[0].DefaultJSON = "1"
		}},
		{"unknown default", "unknown default kind", func(d *ingitdb.CollectionDef) { d.SourceSchema.Fields[0].DefaultKind = "alien" }},
		{"source definition", "source definition", func(d *ingitdb.CollectionDef) { d.SourceSchema.SourceDefinitionJSON = "{" }},
		{"source rights", "source rights", func(d *ingitdb.CollectionDef) { d.SourceSchema.SourceRightsJSON = "{" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := describeSourceCase(t, tc.mutate)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDescribeSourceSchemaSupportedDefaultsAndTypes(t *testing.T) {
	for _, tc := range []struct{ kind, typ, json string }{
		{"current-timestamp", "", ""}, {"literal", "int", "2"}, {"literal", "int64", "2"}, {"literal", "float64", "2.5"}, {"literal", "string", `"a"`}, {"literal", "bool", "true"}, {"literal", "<nil>", "null"},
	} {
		t.Run(tc.kind+tc.typ, func(t *testing.T) {
			got, err := describeSourceCase(t, func(d *ingitdb.CollectionDef) {
				f := &d.SourceSchema.Fields[0]
				f.DefaultKind = tc.kind
				f.DefaultType = tc.typ
				f.DefaultJSON = tc.json
			})
			if err != nil || got.Fields[0].Default == nil {
				t.Fatalf("default: %+v, %v", got, err)
			}
		})
	}
	for _, name := range []string{"bool", "int", "float", "string", "bytes", "time", "decimal"} {
		if _, ok := sourceFieldType(name); !ok {
			t.Fatalf("missing type %s", name)
		}
	}
}

func TestDecodeSingleJSONRequiresOneValue(t *testing.T) {
	for _, bad := range []string{"{", "{} {}"} {
		var out map[string]any
		if err := decodeSingleJSON(bad, &out); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}
