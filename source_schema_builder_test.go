package dalgo2ingitdb

import (
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/datarights"
	"github.com/dal-go/dalgo/dbschema"
	"math"
	"strings"
	"testing"
)

func TestExportCollectionDefinitionDefaultsAndKeyModes(t *testing.T) {
	keyless, err := ExportCollectionDefinition(dbschema.CollectionDef{Name: "events", Fields: []dbschema.FieldDef{{Name: "at", Type: dbschema.Time, Default: dbschema.DefaultCurrentTimestamp{}}}})
	if err != nil || keyless.SourceSchema.KeyMode != "export-ordinal" || keyless.SourceSchema.Fields[0].DefaultKind != "current-timestamp" {
		t.Fatalf("keyless/current timestamp: %+v, %v", keyless, err)
	}
	keyed, err := ExportCollectionDefinition(dbschema.CollectionDef{Name: "events", Fields: []dbschema.FieldDef{{Name: "id", Type: dbschema.Int}}, PrimaryKey: []dal.FieldName{"id"}, SourceRights: []datarights.SourceRight{{SourceID: "original"}}})
	if err != nil || keyed.SourceSchema.KeyMode != "source-primary-key" || keyed.SourceSchema.SourceRightsJSON == "" {
		t.Fatalf("keyed/rights: %+v, %v", keyed, err)
	}
}

func TestExportCollectionDefinitionRejectsBadDefaults(t *testing.T) {
	cases := []struct {
		value dbschema.DefaultExpr
		want  string
	}{
		{dbschema.DefaultLiteral{Value: int32(1)}, "unsupported value type"},
		{dbschema.DefaultLiteral{Value: math.NaN()}, "default"},
		{&dbschema.DefaultLiteral{Value: 1}, "unsupported type"},
	}
	for _, tc := range cases {
		_, err := ExportCollectionDefinition(dbschema.CollectionDef{Name: "bad", Fields: []dbschema.FieldDef{{Name: "x", Type: dbschema.String, Default: tc.value}}})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%T: %v", tc.value, err)
		}
	}
	_, err := ExportCollectionDefinition(dbschema.CollectionDef{Name: "bad", Fields: []dbschema.FieldDef{{Name: "x", Type: dbschema.Type(99)}}})
	if err == nil {
		t.Fatal("unsupported field type accepted")
	}
}
