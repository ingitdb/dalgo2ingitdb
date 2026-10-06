package dalgo2ingitdb

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"gopkg.in/yaml.v3"
)

func TestExportDefinitionRoundTripsRelationalSchema(t *testing.T) {
	length := 64
	source := dbschema.CollectionDef{
		Name: "line_items",
		Fields: []dbschema.FieldDef{
			{Name: "order_id", Type: dbschema.Int},
			{Name: "line_id", Type: dbschema.Int},
			{Name: "amount", Type: dbschema.Decimal, Precision: &dbschema.Precision{Total: 30, Scale: 8}},
			{Name: "payload", Type: dbschema.Bytes, Nullable: true, Default: dbschema.DefaultLiteral{Value: []byte{0, 255}}},
			{Name: "note", Type: dbschema.String, Length: &length},
		},
		PrimaryKey:       []dal.FieldName{"order_id", "line_id"},
		Indexes:          []dbschema.IndexDef{{Name: "by_amount", Collection: "line_items", Fields: []dal.FieldName{"amount"}}},
		ForeignKeys:      []dbschema.ForeignKeyDef{{Fields: []dal.FieldName{"order_id", "line_id"}, ReferencedCollection: "orders", ReferencedFields: []dal.FieldName{"id", "line"}, Enforcement: dbschema.ForeignKeyEnforcementEnabled, OnDelete: "CASCADE", OnUpdate: "RESTRICT"}},
		SourceDefinition: &dbschema.SourceDefinition{Dialect: "sqlite", CreateSQL: "CREATE TABLE line_items(...)"},
	}
	def, err := ExportCollectionDefinition(source)
	if err != nil {
		t.Fatal(err)
	}
	if def.Columns["amount"].Type != "string" || def.Columns["payload"].Type != "string" {
		t.Fatalf("lossy carriers: %#v", def.Columns)
	}
	b, err := yaml.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, source.Name, ".collection"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, source.Name, ".collection", "definition.yaml"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("schema reader unavailable")
	}
	ref := dal.NewRootCollectionRef(source.Name, "")
	got, err := reader.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.PrimaryKey, source.PrimaryKey) || !reflect.DeepEqual(got.Indexes, source.Indexes) || !reflect.DeepEqual(got.ForeignKeys, source.ForeignKeys) {
		t.Fatalf("relational metadata changed: %+v", got)
	}
	if got.SourceDefinition == nil || got.SourceDefinition.CreateSQL != source.SourceDefinition.CreateSQL {
		t.Fatalf("native DDL lost: %+v", got.SourceDefinition)
	}
	if literal, ok := got.Fields[3].Default.(dbschema.DefaultLiteral); !ok || !reflect.DeepEqual(literal.Value, []byte{0, 255}) {
		t.Fatalf("byte default lost: %#v", got.Fields[3].Default)
	}
	if got.Fields[4].Length == nil || *got.Fields[4].Length != length {
		t.Fatalf("length lost: %+v", got.Fields[4])
	}
	listed, err := reader.ListIndexes(context.Background(), &ref)
	if err != nil || !reflect.DeepEqual(listed, source.Indexes) {
		t.Fatalf("ListIndexes = %+v, %v", listed, err)
	}
}

func TestDescribeImportedTypedCSVExcludesTransportID(t *testing.T) {
	source := dbschema.CollectionDef{Name: "items", Fields: []dbschema.FieldDef{{Name: "code", Type: dbschema.String}, {Name: "note", Type: dbschema.String, Nullable: true}}, PrimaryKey: []dal.FieldName{"code"}}
	def, err := ExportCollectionDefinition(source)
	if err != nil {
		t.Fatal(err)
	}
	def.RecordFile = &ingitdb.RecordFileDef{Name: "records.csv", Format: ingitdb.RecordFormatCSV, RecordType: ingitdb.ListOfRecords, CSVCellEncoding: "json-v1"}
	def.ColumnsOrder = append([]string{"$ID"}, def.ColumnsOrder...)
	if err := def.Validate(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "items", ".collection")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "definition.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("schema reader unavailable")
	}
	ref := dal.NewRootCollectionRef("items", "")
	got, err := reader.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Fields, source.Fields) || !reflect.DeepEqual(got.PrimaryKey, source.PrimaryKey) {
		t.Fatalf("CSV source schema changed: %+v", got)
	}
}
