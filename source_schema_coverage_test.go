package dalgo2ingitdb

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"testing"
)

func TestDescribeSourceExportOrdinalAndListIndexError(t *testing.T) {
	got, err := describeSourceCase(t, func(d *ingitdb.CollectionDef) { d.SourceSchema.KeyMode = "export-ordinal" })
	if err != nil || len(got.PrimaryKey) != 0 {
		t.Fatalf("ordinal source PK: %+v, %v", got, err)
	}
	db, err := NewDatabase(t.TempDir(), newReader())
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := dal.As[interface {
		ListIndexes(context.Context, *dal.CollectionRef) ([]dbschema.IndexDef, error)
	}](db)
	if !ok {
		t.Fatal("index reader unavailable")
	}
	if _, err := reader.ListIndexes(context.Background(), nil); err == nil {
		t.Fatal("nil collection ref accepted")
	}
}
