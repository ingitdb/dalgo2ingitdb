package dalgo2ingitdb

import (
	"github.com/ingitdb/ingitdb-go/ingitdb"

	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Holding a lock must not prevent the holder from rewriting, reading or
// deleting the guarded file. On Windows LockFileEx byte-range locks are
// mandatory, so locking the target file itself made every such write fail
// (ingitdb/dalgo2ingitdb#13).
func TestFileLock_HolderCanWriteReadAndDeleteTarget(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "definition.yaml")

	if err := withExclusiveLock(path, func() error {
		return os.WriteFile(path, []byte("first"), 0o644)
	}); err != nil {
		t.Fatalf("create under exclusive lock: %v", err)
	}
	if err := withExclusiveLock(path, func() error {
		return os.WriteFile(path, []byte("second"), 0o644)
	}); err != nil {
		t.Fatalf("overwrite under exclusive lock: %v", err)
	}
	if err := withSharedLock(path, func() error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(data) != "second" {
			t.Errorf("read under shared lock: got %q", data)
		}
		return nil
	}); err != nil {
		t.Fatalf("read under shared lock: %v", err)
	}
	if err := withExclusiveLock(path, func() error {
		return os.Remove(path)
	}); err != nil {
		t.Fatalf("remove under exclusive lock: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("target should be removed, stat err = %v", err)
	}
}

func TestSidecarLockPath(t *testing.T) {
	t.Parallel()
	cache := t.TempDir()
	a, err := sidecarLockPath(cache, filepath.Join("x", "Def.yaml"), true)
	if err != nil {
		t.Fatalf("sidecarLockPath: %v", err)
	}
	b, err := sidecarLockPath(cache, filepath.Join("x", ".", "def.yaml"), true)
	if err != nil {
		t.Fatalf("sidecarLockPath: %v", err)
	}
	if a != b {
		t.Errorf("case-folded equivalent paths must share a lock: %q != %q", a, b)
	}
	c, err := sidecarLockPath(cache, filepath.Join("x", "Def.yaml"), false)
	if err != nil {
		t.Fatalf("sidecarLockPath: %v", err)
	}
	if c == a {
		t.Errorf("case-sensitive path must not alias the folded one")
	}
	if !strings.HasPrefix(a, cache) || !strings.HasSuffix(a, ".lock") {
		t.Errorf("lock path %q should be a .lock file under %q", a, cache)
	}
	if info, err := os.Stat(filepath.Dir(a)); err != nil || !info.IsDir() {
		t.Errorf("lock directory should exist: %v", err)
	}
	if _, err := sidecarLockPath(filepath.Join(cache, "file"), "p", true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	blocker := filepath.Join(cache, "blocker")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := sidecarLockPath(blocker, "p", true); err == nil {
		t.Error("want mkdir error when cache dir is a file")
	}
}

func TestFailedFileLocker(t *testing.T) {
	t.Parallel()
	lk := failedFileLocker{err: os.ErrPermission}
	if err := lk.Lock(); err != os.ErrPermission {
		t.Errorf("Lock: %v", err)
	}
	if err := lk.RLock(); err != os.ErrPermission {
		t.Errorf("RLock: %v", err)
	}
	if err := lk.Unlock(); err != nil {
		t.Errorf("Unlock: %v", err)
	}
}

func TestRequireContainedPath(t *testing.T) {
	t.Parallel()
	base := filepath.Join("db", "col")
	if err := requireContainedPath(base, filepath.Join(base, "$records", "a.yaml")); err != nil {
		t.Errorf("contained path rejected: %v", err)
	}
	for _, p := range []string{filepath.Join(base, "..", "x"), base, filepath.Join("db", "other")} {
		if err := requireContainedPath(base, p); err == nil {
			t.Errorf("path %q escaping %q: want error", p, base)
		}
	}
}

func TestForeignKeyTargetExists_InvalidKeyResolvesNoPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{ID: "parents", DirPath: dir, RecordFile: &ingitdb.RecordFileDef{Name: "{key}", Format: ingitdb.RecordFormatYAML, RecordType: ingitdb.SingleRecord}}
	// With a bare "{key}" template ".." would otherwise resolve to the
	// collection directory itself.
	if err := os.WriteFile(filepath.Join(dir, "definition.yaml"), []byte("x: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"..", "a%2fB", ""} {
		exists, err := foreignKeyTargetExists(colDef, key)
		if exists || err != nil {
			t.Errorf("key %q: exists=%v err=%v, want false,nil", key, exists, err)
		}
	}
}

func TestIsWindowsReservedName(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"con", "CON", "nul.txt", "Aux", "prn ", "com1", "LPT9", "com¹", "COM0"} {
		if !isWindowsReservedName(id) {
			t.Errorf("%q should be reserved", id)
		}
	}
	for _, id := range []string{"console", "com", "com10", "lpt", "nul2", "a.con", "", "conx.txt"} {
		if isWindowsReservedName(id) {
			t.Errorf("%q should not be reserved", id)
		}
	}
}
