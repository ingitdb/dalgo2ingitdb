package dalgo2ingitdb

import (
	"fmt"
	"path/filepath"

	"github.com/dal-go/dalgo/dal"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// resolveScopedCollection resolves the collection definition for a (possibly
// nested) collection identified by its leaf name and the key of its parent
// record, returning a copy of that definition whose DirPath is scoped to the
// concrete parent-record path on disk.
//
// On-disk layout (Option A — mirrors Firestore's document/subcollection nesting
// so a repo is human-browsable): a subcollection's data lives *under* its parent
// record's directory, interleaving parent-record ids with subcollection names:
//
//	spaces/family/contacts/c1.json   (key: spaces/family/contacts/c1)
//	spaces/work/contacts/c1.json     (key: spaces/work/contacts/c1)
//
// This is what keeps records physically scoped by their parent chain: two keys
// that share a leaf collection + record id but differ in their parent chain no
// longer collide.
//
// A top-level collection (parent == nil) resolves exactly as before: a flat
// lookup in def.Collections with the schema-declared DirPath untouched. Only
// nested keys take the scoping path, so existing (non-nested) behaviour is
// unchanged.
//
// The returned CollectionDef is a shallow copy of the schema definition with
// only DirPath overridden; RecordFile, Columns, format, etc. still come from the
// subcollection's own schema so encoding/decoding is unaffected.
func resolveScopedCollection(def *ingitdb.Definition, collection string, parent *dal.Key) (*ingitdb.CollectionDef, error) {
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
	dir := cur.DirPath
	// Walk the intermediate ancestors (root's children, grandchildren, ...).
	for i := 1; i < len(ancestors); i++ {
		parentID := ancestors[i-1].id
		subColID := ancestors[i].col
		sub, ok := cur.SubCollections[subColID]
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: %w: subcollection %q under %q", errCollectionNotInDefinition, subColID, cur.ID)
		}
		dir = filepath.Join(dir, parentID, subColID)
		cur = sub
	}

	// Finally descend into the target collection as a subcollection of the
	// deepest ancestor record.
	parentID := ancestors[len(ancestors)-1].id
	target, ok := cur.SubCollections[collection]
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: %w: subcollection %q under %q", errCollectionNotInDefinition, collection, cur.ID)
	}
	dir = filepath.Join(dir, parentID, collection)

	scoped := *target
	scoped.DirPath = dir
	return &scoped, nil
}
