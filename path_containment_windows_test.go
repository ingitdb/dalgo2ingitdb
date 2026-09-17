//go:build windows

package dalgo2ingitdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/record"
)

// NTFS alternate data streams ("a:b") and reserved device names ("con",
// "NUL.txt") must not be usable as record or parent IDs on Windows: they
// would write to a hidden stream or a device instead of a record file.
func TestDB_WindowsRejectsADSAndReservedDeviceNames(t *testing.T) {
	t.Parallel()
	db, root := setupSingleRecordDB(t)
	ctx := context.Background()
	ids := []string{"a:b", "stream::$DATA", "con", "CON", "nul", "NUL.txt", "aux", "prn", "com1", "LPT9"}
	for _, id := range ids {
		if err := setNamedRecord(ctx, db, record.NewKeyWithID("countries", id)); err == nil {
			t.Errorf("Set %q: want error, got nil", id)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "countries", "$records"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read records dir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("unexpected record file %q", e.Name())
	}
}

func TestDB_WindowsRejectsADSAndReservedDeviceParentIDs(t *testing.T) {
	t.Parallel()
	db, _ := setupSpacesWithContactsSubcollection(t)
	ctx := context.Background()
	for _, parentID := range []string{"a:b", "con", "NUL.txt"} {
		key := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", parentID), "contacts", "c1")
		if err := setNamedRecord(ctx, db, key); err == nil {
			t.Errorf("Set under parent %q: want error, got nil", parentID)
		}
	}
}
