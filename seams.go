package dalgo2ingitdb

import (
	"os"
	"path/filepath"
	"regexp"

	"github.com/gofrs/flock"
	"gopkg.in/yaml.v3"

	"github.com/ingitdb/ingitdb-go/config"
)

// Test seams over os.*/config functions. These hold no state; tests swap them
// to inject failures that are otherwise unreachable (e.g. mkdir/write failures,
// or os.Remove returning ErrNotExist via a TOCTOU race), then restore them.
// A test that swaps a seam must NOT call t.Parallel(), since the swap mutates
// package-level state shared with other tests.
var (
	// osMkdirAll is used by CreateCollection.
	osMkdirAll = os.MkdirAll
	// osReadFile is used by rewriteRecordFiles.
	osReadFile = os.ReadFile
	// osWriteFile is used by writeCollectionDefYAML.
	osWriteFile = os.WriteFile
	// osRemove is used by deleteSingleRecordFile.
	osRemove = os.Remove
	// filepathRel is used by readAllSingleRecords. The seam lets tests reach the
	// error branch, which in production is unreachable because the path argument
	// always comes from filepath.Glob under basePath, so it is always relative to
	// basePath and filepath.Rel never fails.
	filepathRel = filepath.Rel
	// filepathIsAbs is used by validateCollectionName. The seam lets tests reach
	// the absolute-path branch, which in production is unreachable because the
	// earlier path-segment check already rejects names that would be absolute.
	filepathIsAbs = filepath.IsAbs
	// regexpCompile is used by buildKeyExtractor. The seam lets tests reach the
	// compile-error branch, which in production is unreachable because the pattern
	// is assembled from regexp.QuoteMeta-escaped parts and is always valid.
	regexpCompile = regexp.Compile
	// yamlMarshal is used by writeCollectionDefYAML and rewriteRecordFiles.
	// The seam lets tests reach the marshal-error branches, which in production
	// are unreachable for the plain map/struct values passed.
	yamlMarshal = yaml.Marshal
	// writeRootCollections is used by the registry helpers.
	writeRootCollections = config.WriteRootCollectionsToFile
	// readSingleRecord is used by readAllSingleRecords. The seam lets tests
	// reach the found==false branch, which in production only occurs when a
	// globbed file vanishes before the read (a TOCTOU race).
	readSingleRecord = readSingleRecordFile
	// newFileLocker is used by withSharedLock/withExclusiveLock. The seam lets
	// tests inject lock-acquisition failures. *flock.Flock satisfies fileLocker.
	newFileLocker = func(path string) fileLocker { return flock.New(path) }
)
