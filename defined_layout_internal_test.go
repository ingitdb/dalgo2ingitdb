package dalgo2ingitdb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// spacesContactsNotes is spaces > contacts > notes, every level with a {key}
// record name, so every level keeps its records in $records unless
// spacesRecordsDir says otherwise.
func spacesContactsNotes(root string, spacesRecordsDir *string) *ingitdb.Definition {
	rf := func(dir *string) *ingitdb.RecordFileDef {
		return &ingitdb.RecordFileDef{Name: "{key}.yaml", Format: ingitdb.RecordFormatYAML, RecordType: ingitdb.SingleRecord, RecordsDir: dir}
	}
	notes := &ingitdb.CollectionDef{ID: "notes", RecordFile: rf(nil)}
	contacts := &ingitdb.CollectionDef{ID: "contacts", RecordFile: rf(nil), SubCollections: map[string]*ingitdb.CollectionDef{"notes": notes}}
	spaces := &ingitdb.CollectionDef{ID: "spaces", DirPath: filepath.Join(root, "spaces"), RecordFile: rf(spacesRecordsDir),
		SubCollections: map[string]*ingitdb.CollectionDef{"contacts": contacts, "items": notes}}
	return &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"spaces": spaces}}
}

func writeFileAt(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

var notesOfC1 = record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "family"), "contacts", "c1")

// Mislaid data one level below a correctly placed ancestor is reported: the
// contacts are at their defined place, but contact c1's notes sit at
// .../contacts/c1/notes/ instead of .../contacts/$records/c1/notes/.
func TestDefinedLayout_MislaidAtDepthTwo(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFileAt(t, filepath.Join(root, "spaces", "$records", "family", "contacts", "$records", "c1.yaml"))
	writeFileAt(t, filepath.Join(root, "spaces", "$records", "family", "contacts", "c1", "notes", "$records", "n1.yaml"))
	_, err := resolveScopedCollection(spacesContactsNotes(root, nil), "notes", notesOfC1)
	if !errors.Is(err, ErrMislaidSubCollection) {
		t.Fatalf("expected ErrMislaidSubCollection, got %v", err)
	}
	for _, want := range []string{
		"subcollection contacts/notes", `parent record "c1"`,
		filepath.Join("contacts", "c1", "notes") + " holds 1 file(s)",
		filepath.Join("contacts", "$records", "c1", "notes") + " (contacts keeps",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

// The layout earlier driver versions wrote at every level is reported at the
// first level that differs, also when only the deeper instance has files.
func TestDefinedLayout_MislaidAncestorOfDeeperInstance(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFileAt(t, filepath.Join(root, "spaces", "family", "contacts", "c1", "notes", "n1.yaml"))
	_, err := resolveScopedCollection(spacesContactsNotes(root, nil), "notes", notesOfC1)
	if !errors.Is(err, ErrMislaidSubCollection) || !strings.Contains(err.Error(), "subcollection spaces/contacts") {
		t.Fatalf("expected the spaces/contacts instance to be reported, got %v", err)
	}
	// The defined layout resolves.
	ok := t.TempDir()
	writeFileAt(t, filepath.Join(ok, "spaces", "$records", "family", "contacts", "$records", "c1", "notes", "$records", "n1.yaml"))
	col, err := resolveScopedCollection(spacesContactsNotes(ok, nil), "notes", notesOfC1)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(ok, "spaces", "$records", "family", "contacts", "$records", "c1", "notes"); col.DirPath != want {
		t.Errorf("DirPath = %s, want %s", col.DirPath, want)
	}
}

// With records_dir data/items, a parent whose key is "data" does not take
// the parent's own records directory for its mislaid subcollection.
func TestDefinedLayout_NestedRecordsDirNotMislaid(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	nested := "data/items"
	writeFileAt(t, filepath.Join(root, "spaces", "data", "items", "s1.yaml"))
	col, err := resolveScopedCollection(spacesContactsNotes(root, &nested), "items", record.NewKeyWithID("spaces", "data"))
	if err != nil {
		t.Fatalf("false positive: %v", err)
	}
	if want := filepath.Join(root, "spaces", "data", "items", "data", "items"); col.DirPath != want {
		t.Errorf("DirPath = %s, want %s", col.DirPath, want)
	}
}

// An invalid ancestor ID is refused at its own level.
func TestDefinedLayout_InvalidAncestorID(t *testing.T) {
	t.Parallel()
	key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", ".."), "contacts", "c1")
	if _, err := resolveScopedCollection(spacesContactsNotes(t.TempDir(), nil), "notes", key); err == nil || !strings.Contains(err.Error(), `".." cannot be used as a file name`) {
		t.Fatalf("got %v", err)
	}
}
