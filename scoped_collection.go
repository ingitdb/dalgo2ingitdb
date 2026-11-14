package dalgo2ingitdb

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// resolveScopedCollection resolves the collection definition for a (possibly
// nested) collection identified by its leaf name and the key of its parent
// record, returning a copy of that definition whose DirPath is scoped to the
// concrete parent-record path on disk.
//
// On-disk layout: the one the definition defines (ingitdb-go
// subcollection-record-validation#req:subcollection-storage-convention), which
// `ingitdb validate` reads. A subcollection's data lives in a directory named
// by its parent record's key, next to the parent's record files, i.e. below
// the parent's records directory (record_file.records_dir; "$records" by
// default for a {key} name, "." for the data directory itself), interleaving
// parent-record ids with subcollection names:
//
//	spaces/$records/family/contacts/...   (default records_dir; key spaces/family/contacts/c1)
//	spaces/family/contacts/...            (records_dir: ".", the Firestore-mirror layout)
//
// Data found at <parent DirPath>/<key>/<sub>/ while the parent's records_dir
// is not "." (the layout this driver wrote before) is mislaid: resolving that
// instance fails with ErrMislaidSubCollection and the details, so nothing
// reads it as empty or writes a second copy next to it.
//
// This is what keeps records physically scoped by their parent chain: two keys
// that share a leaf collection + record id but differ in their parent chain no
// longer collide.
//
// Parent record IDs are validated and escaped like record file keys (see
// validateRecordPathSegment and recordKeyToFileName), so a parent ID can never
// add or climb directory levels.
//
// A top-level collection (parent == nil) resolves exactly as before: a flat
// lookup in def.Collections with the schema-declared DirPath untouched. Only
// nested keys take the scoping path, so existing (non-nested) behaviour is
// unchanged.
//
// The returned CollectionDef is a shallow copy of the schema definition with
// only DirPath overridden; RecordFile, Columns, format, etc. still come from the
// subcollection's own schema so encoding/decoding is unaffected.
func resolveScopedCollection(def *ingitdb.Definition, collection string, parent *record.Key) (*ingitdb.CollectionDef, error) {
	if def == nil {
		return nil, fmt.Errorf("dalgo2ingitdb: transaction has no loaded definition")
	}

	// Top-level collection: preserve the original flat behaviour verbatim.
	if parent == nil {
		colDef, ok := def.Collections[collection]
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: %w: %q", errCollectionNotInDefinition, collection)
		}
		return colDef, nil
	}

	// Build the ancestor record chain from root down to the immediate parent.
	// Each step is a (collection, recordID) pair describing one ancestor record.
	type step struct {
		col string
		id  string
	}
	var ancestors []step
	for k := parent; k != nil; k = k.Parent() {
		ancestors = append([]step{{col: k.Collection(), id: fmt.Sprintf("%v", k.ID)}}, ancestors...)
	}

	// Descend from the root collection, interleaving parent-record ids and
	// subcollection names into the on-disk path.
	rootCol := ancestors[0].col
	cur, ok := def.Collections[rootCol]
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: %w: %q", errCollectionNotInDefinition, rootCol)
	}
	rootDir := cur.DirPath
	dir := rootDir
	legacy := rootDir // where earlier driver versions put the same instance
	for _, a := range ancestors {
		if err := validateRecordPathSegment(a.id); err != nil {
			return nil, err
		}
	}
	// Walk the intermediate ancestors (root's children, grandchildren, ...).
	for i := 1; i < len(ancestors); i++ {
		parentID := ancestors[i-1].id
		subColID := ancestors[i].col
		sub, ok := cur.SubCollections[subColID]
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: %w: subcollection %q under %q", errCollectionNotInDefinition, subColID, cur.ID)
		}
		dir = filepath.Join(dir, recordsBasePath(cur), recordKeyToFileName(parentID), subColID)
		legacy = filepath.Join(legacy, recordKeyToFileName(parentID), subColID)
		cur = sub
	}

	// Finally descend into the target collection as a subcollection of the
	// deepest ancestor record.
	parentID := ancestors[len(ancestors)-1].id
	target, ok := cur.SubCollections[collection]
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: %w: subcollection %q under %q", errCollectionNotInDefinition, collection, cur.ID)
	}
	base := recordsBasePath(cur)
	dir = filepath.Join(dir, base, recordKeyToFileName(parentID), collection)
	legacy = filepath.Join(legacy, recordKeyToFileName(parentID), collection)
	if err := requireContainedPath(rootDir, dir); err != nil {
		return nil, err
	}
	// legacy has the same validated segments as dir minus the records
	// directories, so it is contained whenever dir is.
	if legacy != dir {
		if files := countFiles(legacy); files > 0 {
			return nil, fmt.Errorf("dalgo2ingitdb: %w: subcollection %s/%s of parent record %q: %s holds %d file(s), but the definition puts them at %s (%s keeps its records in record_file.records_dir %q). Move them there, or set record_file.records_dir: \".\" on %s and move its own records out of its records directory",
				ErrMislaidSubCollection, cur.ID, collection, parentID, legacy, files, dir, cur.ID, base, cur.ID)
		}
	}

	scoped := *target
	scoped.DirPath = dir
	return &scoped, nil
}

// ErrMislaidSubCollection reports subcollection records found where the
// definition does not put them (see resolveScopedCollection).
var ErrMislaidSubCollection = errors.New("subcollection records outside the location the definition defines")

// recordsBasePath is the directory, below a collection's DirPath, that holds
// its record files and its records' subcollection directories.
func recordsBasePath(colDef *ingitdb.CollectionDef) string {
	if colDef.RecordFile == nil {
		return ""
	}
	return colDef.RecordFile.RecordsBasePath()
}

// countFiles counts the non-hidden files below dir, at any depth; a missing
// directory has none.
func countFiles(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), ".") {
			n++
		}
		return nil
	})
	return n
}
