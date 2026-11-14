package dalgo2ingitdb

// specscore: feature/dalgo2ingitdb-dbschema-ddl-coverage

import (
	"fmt"
	"strings"

	"github.com/dal-go/dalgo/dbschema"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// ingitdbTypeToDBSchema converts an ingitdb column type to its portable
// dbschema.Type equivalent. Returns an error if the type cannot be
// represented.
//
// The mapping is many-to-one for time types (date, time, datetime all map
// to dbschema.Time) and for non-scalar types (any, map[...]string all map
// to dbschema.String; so do list types such as []string and []any, which hold
// JSON arrays as map types hold JSON objects). Round-tripping through dbschemaTypeToIngitdb is
// therefore lossy: a date column read back as dbschema.Time then written
// back to ingitdb becomes datetime.
func ingitdbTypeToDBSchema(t ingitdb.ColumnType) (dbschema.Type, error) {
	switch t {
	case ingitdb.ColumnTypeString:
		return dbschema.String, nil
	case ingitdb.ColumnTypeInt:
		return dbschema.Int, nil
	case ingitdb.ColumnTypeFloat:
		return dbschema.Float, nil
	case ingitdb.ColumnTypeBool:
		return dbschema.Bool, nil
	case ingitdb.ColumnTypeDate, ingitdb.ColumnTypeTime, ingitdb.ColumnTypeDateTime:
		return dbschema.Time, nil
	case ingitdb.ColumnTypeAny, ingitdb.ColumnTypeL10N:
		return dbschema.String, nil
	case "":
		return dbschema.Null, fmt.Errorf("empty ingitdb column type")
	}
	if strings.HasPrefix(string(t), "map[") {
		return dbschema.String, nil
	}
	if elem, isList := strings.CutPrefix(string(t), "[]"); isList && validListElement(elem) {
		return dbschema.String, nil
	}
	return dbschema.Null, fmt.Errorf("unsupported ingitdb column type: %q", t)
}

// dbschemaTypeToIngitdb converts a portable dbschema.Type to its ingitdb
// column-type equivalent. Returns an error only for types ingitdb cannot
// represent at all (dbschema.Null).
//
// Lossy mappings (documented per case):
//
//   - dbschema.Time → ingitdb.ColumnTypeDateTime (the most general time
//     type). Callers that need date- or time-of-day-only granularity must
//     override the type manually after a round-trip.
//   - dbschema.Decimal → ingitdb.ColumnTypeString. Native storage preserves
//     exact decimal text, and source_schema retains the logical decimal type.
//   - dbschema.Bytes → ingitdb.ColumnTypeString. inGitDB stores binary
//     data base64-encoded in string columns (caller-side encoding;
//     the column type is the carrier).
func dbschemaTypeToIngitdb(t dbschema.Type) (ingitdb.ColumnType, error) {
	switch t {
	case dbschema.Bool:
		return ingitdb.ColumnTypeBool, nil
	case dbschema.Int:
		return ingitdb.ColumnTypeInt, nil
	case dbschema.Float:
		return ingitdb.ColumnTypeFloat, nil
	case dbschema.Decimal:
		// Preserve the exact decimal text; source_schema records its logical type.
		return ingitdb.ColumnTypeString, nil
	case dbschema.String:
		return ingitdb.ColumnTypeString, nil
	case dbschema.Bytes:
		// Lossy: callers base64-encode the value into the string carrier.
		return ingitdb.ColumnTypeString, nil
	case dbschema.Time:
		return ingitdb.ColumnTypeDateTime, nil
	case dbschema.Null:
		return "", fmt.Errorf("dbschema.Null cannot be mapped to an ingitdb column type")
	}
	return "", fmt.Errorf("unsupported dbschema.Type: %s", t)
}

// validListElement reports whether elem, the element type of a list column
// type []elem, is itself a column type: a scalar or list type that
// ingitdbTypeToDBSchema accepts, or a map type written map[K]V.
func validListElement(elem string) bool {
	if rest, isMap := strings.CutPrefix(elem, "map["); isMap {
		key, value, found := strings.Cut(rest, "]")
		return found && key != "" && value != ""
	}
	_, err := ingitdbTypeToDBSchema(ingitdb.ColumnType(elem))
	return err == nil
}
