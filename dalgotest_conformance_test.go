package dalgo2ingitdb_test

import (
	"context"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"

	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// TestConformance runs the shared dalgotest.RunConformance suite against a
// Database rooted at a fresh t.TempDir() project — no live infrastructure or
// credentials required. It genuinely passes: unlike the dalgo2ingitdb4github /
// dalgo2ingitdb4local siblings (whose write paths type-assert record.Data()
// straight to map[string]any and so reject the suite's typed struct
// fixtures), this adapter already converts via record.DataToMap (see
// tx_readwrite.go), which is a no-op for a map and a JSON round-trip for any
// other struct — exactly what the suite's dalgotest.Record / dalgotest.Plain
// fixtures need.
func TestConformance(t *testing.T) {
	dalgotest.RunConformance(t, func(t *testing.T) (dal.DB, func()) {
		root := t.TempDir()
		db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
		if err != nil {
			t.Fatalf("NewDatabase: %v", err)
		}
		modifier, _ := dal.As[ddl.SchemaModifier](db)
		col := dbschema.CollectionDef{
			Name: dalgotest.DefaultCollection,
			Fields: []dbschema.FieldDef{
				{Name: "name", Type: dbschema.String, Nullable: true},
			},
		}
		if err := modifier.CreateCollection(context.Background(), col); err != nil {
			t.Fatalf("CreateCollection: %v", err)
		}
		registerRootCollection(t, root, dalgotest.DefaultCollection, dalgotest.DefaultCollection)
		return db, nil
	})
}
