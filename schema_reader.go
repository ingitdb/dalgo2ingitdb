package dalgo2ingitdb

// specscore: feature/dalgo2ingitdb-dbschema-ddl-coverage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/datarights"
	"github.com/dal-go/dalgo/dbschema"
	"gopkg.in/yaml.v3"

	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// pkFieldName is the synthesized primary-key field name used to describe
// the inGitDB record-key convention. inGitDB does not declare PKs in
// definition.yaml; each record's filesystem key is the de-facto PK.
const pkFieldName dal.FieldName = "$key"

// reservedSubDirs are directory names that MUST NOT be traversed as
// candidate collection roots during ListCollections — they hold
// auxiliary state (schema, records, subcollections, views) and never
// contain a sibling .collection/definition.yaml of their own.
var reservedSubDirs = map[string]bool{
	ingitdb.SchemaDir:      true, // .collection
	"$records":             true,
	"subcollections":       true,
	"views":                true,
	ingitdb.SharedViewsDir: true, // $views
}

// ListCollections walks the project directory looking for directories
// that contain a .collection/definition.yaml file. The parent argument
// is ignored (inGitDB has no catalog hierarchy). Results are sorted
// alphabetically by name; names use "/" as the separator for nested
// collection paths relative to projectPath.
func (db *Database) ListCollections(_ context.Context, _ *record.Key) ([]dal.CollectionRef, error) {
	var refs []dal.CollectionRef
	walkErr := filepath.WalkDir(db.projectPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		// Skip reserved sub-directory names anywhere in the tree.
		base := d.Name()
		if path != db.projectPath && reservedSubDirs[base] {
			return fs.SkipDir
		}
		// Check whether this directory is a collection root.
		defPath := filepath.Join(path, ingitdb.SchemaDir, ingitdb.CollectionDefFileName)
		info, statErr := os.Stat(defPath)
		if statErr != nil || info.IsDir() {
			return nil
		}
		rel, relErr := filepathRel(db.projectPath, path)
		if relErr != nil {
			return fmt.Errorf("relative path for %s: %w", path, relErr)
		}
		if rel == "." {
			return nil
		}
		name := filepath.ToSlash(rel)
		refs = append(refs, dal.NewRootCollectionRef(name, ""))
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walk %s: %w", db.projectPath, walkErr)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name() < refs[j].Name() })
	return refs, nil
}

// DescribeCollection reads and parses the collection's definition.yaml
// under a shared lock, then maps the ingitdb columns to dbschema fields
// via type_mapping. PrimaryKey is synthesized as [pkFieldName] because
// inGitDB uses the record's filesystem key as the de-facto PK.
func (db *Database) DescribeCollection(_ context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if ref == nil {
		return nil, fmt.Errorf("dalgo2ingitdb: DescribeCollection: ref is nil")
	}
	name := ref.Name()
	defPath := filepath.Join(db.projectPath, name, ingitdb.SchemaDir, ingitdb.CollectionDefFileName)

	if _, err := os.Stat(defPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("dalgo2ingitdb: collection %q not found: %w", name, err)
		}
		return nil, fmt.Errorf("dalgo2ingitdb: stat definition.yaml for %q: %w", name, err)
	}

	var colDef ingitdb.CollectionDef
	readErr := withSharedLock(defPath, func() error {
		content, err := osReadFile(defPath)
		if err != nil {
			return fmt.Errorf("read definition.yaml: %w", err)
		}
		if err := yaml.Unmarshal(content, &colDef); err != nil {
			return fmt.Errorf("parse definition.yaml: %w", err)
		}
		return nil
	})
	if readErr != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: describe %q: %w", name, readErr)
	}

	fieldOrder := colDef.ColumnsOrder
	if len(fieldOrder) == 0 {
		fieldOrder = make([]string, 0, len(colDef.Columns))
		for colName := range colDef.Columns {
			fieldOrder = append(fieldOrder, colName)
		}
		sort.Strings(fieldOrder)
	}

	fields := make([]dbschema.FieldDef, 0, len(fieldOrder))
	for _, colName := range fieldOrder {
		if colName == "$ID" && colDef.RecordFile != nil && colDef.RecordFile.Format == ingitdb.RecordFormatCSV && colDef.RecordFile.CSVCellEncoding == "json-v1" && colDef.SourceSchema != nil && colDef.SourceSchema.KeyMode != "" {
			// Imported typed CSV carries the transport key in its first cell.
			// It is not a source field or a native data column.
			continue
		}
		if colName == string(pkFieldName) {
			// Never expose the synthesized PK as a regular field.
			continue
		}
		col, ok := colDef.Columns[colName]
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: describe %q: columns_order references unknown column %q", name, colName)
		}
		t, err := ingitdbTypeToDBSchema(col.Type)
		if err != nil {
			return nil, fmt.Errorf("dalgo2ingitdb: describe %q field %q: %w", name, colName, err)
		}
		fields = append(fields, dbschema.FieldDef{
			Name:     dal.FieldName(colName),
			Type:     t,
			Nullable: !col.Required,
		})
	}
	indexes := []dbschema.IndexDef{}
	var foreignKeys []dbschema.ForeignKeyDef
	var sourceDefinition *dbschema.SourceDefinition
	var sourceRights []datarights.SourceRight
	if schema := colDef.SourceSchema; schema != nil {
		// Generic DDL edits can add or drop fields after creation. Exported
		// snapshots are immutable and must retain exact source-field order.
		strictSource := schema.KeyMode != ""
		sourceFieldsMatch := len(schema.Fields) == len(fields)
		if sourceFieldsMatch {
			for i, sourceField := range schema.Fields {
				if sourceField.Name != string(fields[i].Name) {
					sourceFieldsMatch = false
					break
				}
			}
		}
		if strictSource && len(schema.Fields) != len(fields) {
			return nil, fmt.Errorf("dalgo2ingitdb: describe %q: source schema fields do not match columns", name)
		}
		fieldPositions := make(map[string]int, len(fields))
		for i, field := range fields {
			fieldPositions[string(field.Name)] = i
		}
		if strictSource || sourceFieldsMatch {
			for i, sourceField := range schema.Fields {
				fieldIndex, exists := fieldPositions[sourceField.Name]
				if !exists {
					return nil, fmt.Errorf("dalgo2ingitdb: describe %q: source schema field %q missing from columns", name, sourceField.Name)
				}
				if fieldIndex != i {
					return nil, fmt.Errorf("dalgo2ingitdb: describe %q: source schema field %q out of order", name, sourceField.Name)
				}
				field := &fields[fieldIndex]
				if t, ok := sourceFieldType(sourceField.Type); ok {
					field.Type = t
				} else {
					return nil, fmt.Errorf("dalgo2ingitdb: describe %q: unsupported source type %q", name, sourceField.Type)
				}
				field.Nullable = sourceField.Nullable
				field.AutoIncrement = sourceField.AutoIncrement
				field.Length = sourceField.Length
				switch sourceField.DefaultKind {
				case "":
				case "current-timestamp":
					field.Default = dbschema.DefaultCurrentTimestamp{}
				case "literal":
					var value any
					decoder := json.NewDecoder(strings.NewReader(sourceField.DefaultJSON))
					decoder.UseNumber()
					if err := decoder.Decode(&value); err != nil {
						return nil, fmt.Errorf("dalgo2ingitdb: describe %q default for %q: %w", name, sourceField.Name, err)
					}
					var trailing any
					if err := decoder.Decode(&trailing); err != io.EOF {
						return nil, fmt.Errorf("dalgo2ingitdb: describe %q default for %q has trailing content", name, sourceField.Name)
					}
					switch sourceField.DefaultType {
					case "[]uint8":
						var binary []byte
						if err := json.Unmarshal([]byte(sourceField.DefaultJSON), &binary); err != nil {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q byte default for %q: %w", name, sourceField.Name, err)
						}
						value = binary
					case "int", "int64":
						number, ok := value.(json.Number)
						if !ok {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q integer default for %q is not numeric", name, sourceField.Name)
						}
						i, err := number.Int64()
						if err != nil {
							return nil, err
						}
						if sourceField.DefaultType == "int" {
							value = int(i)
						} else {
							value = i
						}
					case "<nil>":
						if value != nil {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q nil default for %q has value", name, sourceField.Name)
						}
					case "string":
						if _, ok := value.(string); !ok {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q string default for %q is not a string", name, sourceField.Name)
						}
					case "bool":
						if _, ok := value.(bool); !ok {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q bool default for %q is not a bool", name, sourceField.Name)
						}
					case "float64":
						number, ok := value.(json.Number)
						if !ok {
							return nil, fmt.Errorf("dalgo2ingitdb: describe %q float default for %q is not numeric", name, sourceField.Name)
						}
						f, err := number.Float64()
						if err != nil {
							return nil, err
						}
						value = f
					default:
						return nil, fmt.Errorf("dalgo2ingitdb: describe %q unknown default literal type %q", name, sourceField.DefaultType)
					}
					field.Default = dbschema.DefaultLiteral{Value: value}
				default:
					return nil, fmt.Errorf("dalgo2ingitdb: describe %q unknown default kind %q", name, sourceField.DefaultKind)
				}
				if sourceField.Precision > 0 {
					field.Precision = &dbschema.Precision{Total: sourceField.Precision, Scale: sourceField.Scale}
				}
			}
		}
		for _, idx := range schema.Indexes {
			entry := dbschema.IndexDef{Name: idx.Name, Collection: name, Unique: idx.Unique}
			for _, f := range idx.Fields {
				entry.Fields = append(entry.Fields, dal.FieldName(f))
			}
			indexes = append(indexes, entry)
		}
		for _, fk := range schema.ForeignKeys {
			entry := dbschema.ForeignKeyDef{Name: fk.Name, ReferencedCollection: fk.ReferencedCollection,
				ReferencedNamespace: fk.ReferencedNamespace, Enforcement: dbschema.ForeignKeyEnforcement(fk.SourceEnforcement),
				OnUpdate: fk.OnUpdate, OnDelete: fk.OnDelete}
			for _, f := range fk.Fields {
				entry.Fields = append(entry.Fields, dal.FieldName(f))
			}
			for _, f := range fk.ReferencedFields {
				entry.ReferencedFields = append(entry.ReferencedFields, dal.FieldName(f))
			}
			foreignKeys = append(foreignKeys, entry)
		}
		if schema.SourceDefinitionJSON != "" {
			sourceDefinition = new(dbschema.SourceDefinition)
			if err := decodeSingleJSON(schema.SourceDefinitionJSON, sourceDefinition); err != nil {
				return nil, fmt.Errorf("dalgo2ingitdb: describe %q source definition: %w", name, err)
			}
		}
		if schema.SourceRightsJSON != "" {
			if err := decodeSingleJSON(schema.SourceRightsJSON, &sourceRights); err != nil {
				return nil, fmt.Errorf("dalgo2ingitdb: describe %q source rights: %w", name, err)
			}
		}
	}

	pk := primaryKeyFromColDef(colDef)
	if colDef.SourceSchema != nil && colDef.SourceSchema.KeyMode == "export-ordinal" {
		pk = nil
	}
	return &dbschema.CollectionDef{
		Name:             name,
		Fields:           fields,
		PrimaryKey:       pk,
		Indexes:          indexes,
		ForeignKeys:      foreignKeys,
		SourceDefinition: sourceDefinition,
		SourceRights:     sourceRights,
	}, nil
}

func sourceFieldType(name string) (dbschema.Type, bool) {
	switch name {
	case "bool":
		return dbschema.Bool, true
	case "int":
		return dbschema.Int, true
	case "float":
		return dbschema.Float, true
	case "string":
		return dbschema.String, true
	case "bytes":
		return dbschema.Bytes, true
	case "time":
		return dbschema.Time, true
	case "decimal":
		return dbschema.Decimal, true
	default:
		return dbschema.Null, false
	}
}

// primaryKeyFromColDef extracts the persisted PrimaryKey column names from
// definition.yaml when present, falling back to the synthesized [pkFieldName]
// for legacy projects that predate PK persistence. See REQ:describe-collection.
func primaryKeyFromColDef(colDef ingitdb.CollectionDef) []dal.FieldName {
	if len(colDef.PrimaryKey) == 0 {
		return []dal.FieldName{pkFieldName}
	}
	pk := make([]dal.FieldName, len(colDef.PrimaryKey))
	for i, name := range colDef.PrimaryKey {
		pk[i] = dal.FieldName(name)
	}
	return pk
}

// ListIndexes returns source index declarations retained by an export.
// They are descriptive: inGitDB does not maintain a native secondary index.
func (db *Database) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	def, err := db.DescribeCollection(ctx, ref)
	if err != nil {
		return nil, err
	}
	return def.Indexes, nil
}

// ListConstraints returns a synthesized single-element slice describing
// the primary-key constraint. ingitdb does not store other constraint
// kinds in definition.yaml. The richer PK column information lives on
// `DescribeCollection.PrimaryKey`; dbschema.ConstraintDef is intentionally
// minimal (Name + Type only).
func (db *Database) ListConstraints(_ context.Context, _ *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return []dbschema.ConstraintDef{{Name: "$key-pk", Type: "primary-key"}}, nil
}

// ListReferrers returns *dbschema.NotSupportedError — inGitDB has no
// structural foreign-key declarations; ColumnDef.ForeignKey is a
// free-text hint, not a navigable reference.
func (db *Database) ListReferrers(_ context.Context, _ *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return nil, &dbschema.NotSupportedError{
		Op:      "ListReferrers",
		Backend: DatabaseID,
		Reason:  "inGitDB has no native foreign-key declarations",
	}
}

// Compile-time check: *Database satisfies dbschema.SchemaReader.
var _ dbschema.SchemaReader = (*Database)(nil)

func decodeSingleJSON(input string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(input))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}
