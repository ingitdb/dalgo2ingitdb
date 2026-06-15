package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dbschema"

	ingitdb "github.com/ingitdb/ingitdb-go"
)

// errSeam is a sentinel error injected by seam-swapping tests in this file.
var errSeam = errors.New("seam failure")

// The tests below swap package-level seams from seams.go to reach error
// branches that are unreachable in production (marshal/compile/path failures on
// already-validated, in-memory, or glob-derived inputs). Each is intentionally
// NOT parallel because it mutates package-level state.

// TestWriteCollectionDefYAML_MarshalError covers the yaml.Marshal error branch
// in writeCollectionDefYAML (schema_modifier.go line 365-367) via yamlMarshal.
func TestWriteCollectionDefYAML_MarshalError(t *testing.T) {
	orig := yamlMarshal
	yamlMarshal = func(any) ([]byte, error) { return nil, errSeam }
	defer func() { yamlMarshal = orig }()

	p := filepath.Join(t.TempDir(), "definition.yaml")
	err := writeCollectionDefYAML(p, &ingitdb.CollectionDef{ID: "c"})
	if err == nil {
		t.Fatal("writeCollectionDefYAML: want error when yamlMarshal fails")
	}
	if !strings.Contains(err.Error(), "marshal definition.yaml") {
		t.Errorf("error = %v, want it to wrap the marshal failure", err)
	}
}

// TestRewriteRecordFiles_MarshalError covers the yaml.Marshal error branch
// inside the walk in rewriteRecordFiles (schema_modifier.go line 422-424) via
// the yamlMarshal seam. A real record file is present so the walk reaches the
// marshal step after a successful read+unmarshal.
func TestRewriteRecordFiles_MarshalError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "rec.yaml"), []byte("a: 1\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	orig := yamlMarshal
	yamlMarshal = func(any) ([]byte, error) { return nil, errSeam }
	defer func() { yamlMarshal = orig }()

	err := rewriteRecordFiles(root, ingitdb.RecordFormatYAML, func(_ map[string]any) {})
	if err == nil {
		t.Fatal("rewriteRecordFiles: want error when yamlMarshal fails")
	}
	if !strings.Contains(err.Error(), "marshal ") {
		t.Errorf("error = %v, want it to wrap the marshal failure", err)
	}
}

// TestValidateCollectionName_IsAbs covers the filepath.IsAbs branch in
// validateCollectionName (schema_modifier.go line 316-318) via the filepathIsAbs
// seam. In production the earlier path-segment check rejects names that would be
// absolute, so this branch cannot be reached with a real name.
func TestValidateCollectionName_IsAbs(t *testing.T) {
	orig := filepathIsAbs
	filepathIsAbs = func(string) bool { return true }
	defer func() { filepathIsAbs = orig }()

	err := validateCollectionName("valid-name")
	if err == nil {
		t.Fatal("validateCollectionName: want error when filepath.IsAbs reports absolute")
	}
	if !strings.Contains(err.Error(), "must be relative") {
		t.Errorf("error = %v, want the must-be-relative error", err)
	}
}

// TestListCollections_RelError covers the filepath.Rel error branch in
// ListCollections (schema_reader.go line 63-65) via the filepathRel seam. In
// production the walked path is always under projectPath, so filepath.Rel never
// fails.
func TestListCollections_RelError(t *testing.T) {
	root := t.TempDir()
	writeCollectionDef(t, root, "tags", countriesDef)

	orig := filepathRel
	filepathRel = func(string, string) (string, error) { return "", errSeam }
	defer func() { filepathRel = orig }()

	db, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	reader := db.(dbschema.SchemaReader)
	_, err = reader.ListCollections(context.Background(), nil)
	if err == nil {
		t.Fatal("ListCollections: want error when filepath.Rel fails")
	}
}

// TestReadAllSingleRecords_CompileError covers the regexp.Compile error branch
// in buildKeyExtractor (query.go line 148-150) and its propagation in
// readAllSingleRecords (query.go line 86-88) via the regexpCompile seam. The
// record-file name contains {key}, so buildKeyExtractor takes the regexp path.
func TestReadAllSingleRecords_CompileError(t *testing.T) {
	orig := regexpCompile
	regexpCompile = func(string) (*regexp.Regexp, error) { return nil, errSeam }
	defer func() { regexpCompile = orig }()

	colDef := &ingitdb.CollectionDef{
		ID:      "things",
		DirPath: t.TempDir(),
		RecordFile: &ingitdb.RecordFileDef{
			Name:       "{key}.yaml",
			Format:     ingitdb.RecordFormatYAML,
			RecordType: ingitdb.SingleRecord,
		},
	}
	_, err := readAllSingleRecords(colDef)
	if err == nil {
		t.Fatal("readAllSingleRecords: want error when regexp.Compile fails")
	}
	if !strings.Contains(err.Error(), "build key extractor") {
		t.Errorf("error = %v, want it to wrap the compile failure", err)
	}
}
