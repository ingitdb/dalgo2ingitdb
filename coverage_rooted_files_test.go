package dalgo2ingitdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootedFiles_OpenRootedFilesWithProject_FilepathAbsError(t *testing.T) {
	origAbs := filepathAbs
	defer func() { filepathAbs = origAbs }()
	filepathAbs = func(path string) (string, error) {
		return "", errors.New("filepathAbs fail")
	}
	_, err := openRootedFilesWithProject("some-dir", RootedFilesScope{Prefix: "scope"}, os.OpenRoot, nil)
	if err == nil || !strings.Contains(err.Error(), "filepathAbs fail") {
		t.Fatalf("openRootedFilesWithProject want filepathAbs error, got %v", err)
	}
}

func TestRootedFiles_EnsureDir_Gaps(t *testing.T) {
	// 1. closed beginOperation error (line 368)
	fClosed := &RootedFiles{closed: true}
	if err := fClosed.EnsureDir("sub", 0o755); err == nil {
		t.Fatal("EnsureDir closed want error")
	}

	// 2. scopedRelativePath error (line 375)
	fEmpty := &RootedFiles{}
	if err := fEmpty.EnsureDir("../escape", 0o755); err == nil {
		t.Fatal("EnsureDir escape want error")
	}

	// 3. ensureTopLevelDir rootHandle error (line 388)
	if err := fEmpty.ensureTopLevelDir("sub", 0o755); err == nil {
		t.Fatal("ensureTopLevelDir rootHandle want error")
	}

	dir := t.TempDir()

	// 4. root.Mkdir permission error (line 395)
	readOnly := filepath.Join(dir, "ro")
	if err := os.Mkdir(readOnly, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(readOnly, 0o755) }()
	roRoot, err := os.OpenRoot(readOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = roRoot.Close() }()
	fRO := &RootedFiles{root: roRoot}
	if err := fRO.ensureTopLevelDir("newdir", 0o755); err == nil {
		t.Fatal("ensureTopLevelDir mkdir in read-only want error")
	}

	// 5. root.Lstat error with closed root (line 401)
	closedRoot, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = closedRoot.Close()
	fClosedRoot := &RootedFiles{root: closedRoot}
	if err := fClosedRoot.ensureTopLevelDir("sub", 0o755); err == nil {
		t.Fatal("ensureTopLevelDir closed root want error")
	}

	// 6. root.Open permission error (line 408)
	noPermDir := filepath.Join(dir, "noperm")
	if err := os.Mkdir(noPermDir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(noPermDir, 0o755) }()
	normalRoot, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = normalRoot.Close() }()
	fNormal := &RootedFiles{root: normalRoot}
	if err := fNormal.ensureTopLevelDir("noperm", 0o755); err == nil {
		t.Fatal("ensureTopLevelDir open 0o000 want error")
	}

	// 7. sameFileSeam swap error (lines 412, 423)
	subDir := filepath.Join(dir, "swapped")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	origSame := sameFileSeam
	defer func() { sameFileSeam = origSame }()
	sameFileSeam = func(info1, info2 os.FileInfo) bool { return false }
	if err := fNormal.ensureTopLevelDir("swapped", 0o755); err == nil || !strings.Contains(err.Error(), "changed during acquisition") {
		t.Fatalf("ensureTopLevelDir swapped want changed error, got %v", err)
	}
	sameFileSeam = origSame

	// 8. syncDir error on created "." (line 427)
	fSyncParentFail := &RootedFiles{
		root: normalRoot,
		syncDirectory: func(r *os.Root, rel string) error {
			if rel == "." {
				return errors.New("parent sync fail")
			}
			return nil
		},
	}
	if err := fSyncParentFail.ensureTopLevelDir("newdir1", 0o755); err == nil || !strings.Contains(err.Error(), "parent sync fail") {
		t.Fatalf("ensureTopLevelDir want parent sync fail, got %v", err)
	}

	// 9. syncDir error on created child (line 430)
	fSyncChildFail := &RootedFiles{
		root: normalRoot,
		syncDirectory: func(r *os.Root, rel string) error {
			if rel != "." {
				return errors.New("child sync fail")
			}
			return nil
		},
	}
	if err := fSyncChildFail.ensureTopLevelDir("newdir2", 0o755); err == nil || !strings.Contains(err.Error(), "child sync fail") {
		t.Fatalf("ensureTopLevelDir want child sync fail, got %v", err)
	}

	// 10. syncDir error on modeChanged (line 434)
	existingMode := filepath.Join(dir, "mode_dir")
	if err := os.Mkdir(existingMode, 0o755); err != nil {
		t.Fatal(err)
	}
	fSyncModeFail := &RootedFiles{
		root: normalRoot,
		syncDirectory: func(r *os.Root, rel string) error {
			if rel == "mode_dir" {
				return errors.New("mode sync fail")
			}
			return nil
		},
	}
	if err := fSyncModeFail.ensureTopLevelDir("mode_dir", 0o700); err == nil || !strings.Contains(err.Error(), "mode sync fail") {
		t.Fatalf("ensureTopLevelDir want mode sync fail, got %v", err)
	}
}

func TestRootedFiles_JSONL_Gaps(t *testing.T) {
	// 1. ReadJSONLWithLimit closed (line 506)
	fClosed := &RootedFiles{closed: true}
	if _, err := fClosed.ReadJSONLWithLimit("p", 10); err == nil {
		t.Fatal("ReadJSONLWithLimit closed want error")
	}

	// 2. recoverAndReadJSONL invalid maxBytes (line 542)
	f := &RootedFiles{}
	if _, err := f.recoverAndReadJSONL("p", 0); err == nil {
		t.Fatal("recoverAndReadJSONL 0 want error")
	}
	if _, err := f.recoverAndReadJSONL("p", -2); err == nil {
		t.Fatal("recoverAndReadJSONL -2 want error")
	}

	// 3. recoverAndReadJSONL buffer size error (line 546)
	origBufSize := rootedJSONLBufferSizeSeam
	defer func() { rootedJSONLBufferSizeSeam = origBufSize }()
	rootedJSONLBufferSizeSeam = func(maxBytes int64) (int, error) {
		return 0, errors.New("buf size fail")
	}
	if _, err := f.recoverAndReadJSONL("p", 10); err == nil || !strings.Contains(err.Error(), "buf size fail") {
		t.Fatalf("recoverAndReadJSONL want buf size fail, got %v", err)
	}
	rootedJSONLBufferSizeSeam = origBufSize

	// 4. recoverAndReadJSONL bad path (line 553)
	if _, err := f.recoverAndReadJSONL("../bad", 10); err == nil {
		t.Fatal("recoverAndReadJSONL bad path want error")
	}

	// 5. recoverAndReadJSONL openFile error (line 557)
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	fReal := &RootedFiles{root: root, fileOps: defaultRootedFileOps()}
	if _, err := fReal.recoverAndReadJSONL("nonexistent.jsonl", 10); err == nil {
		t.Fatal("recoverAndReadJSONL nonexistent want error")
	}
}

func TestRootedFiles_ReadJSON_WithExclusiveLock_ReadDir_Gaps(t *testing.T) {
	f := &RootedFiles{}

	// 1. readJSON rootHandle error (line 678)
	var dummy map[string]any
	if err := f.readJSON("p", &dummy); err == nil {
		t.Fatal("readJSON rootHandle want error")
	}

	// 2. WithExclusiveLock rootHandle error (line 720)
	if err := f.WithExclusiveLock(context.Background(), "p", func(LockedFiles) error { return nil }); err == nil {
		t.Fatal("WithExclusiveLock rootHandle want error")
	}

	// 3. WithExclusiveLock stat error with closed root (line 725)
	dir := t.TempDir()
	closedRoot, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = closedRoot.Close()
	fClosedRoot := &RootedFiles{root: closedRoot}
	if err := fClosedRoot.WithExclusiveLock(context.Background(), "p", func(LockedFiles) error { return nil }); err == nil {
		t.Fatal("WithExclusiveLock closed root want error")
	}

	// 4. readDir rootHandle error (line 769)
	if _, err := f.readDir("p"); err == nil {
		t.Fatal("readDir rootHandle want error")
	}

	// 5. readDir on regular file (line 780)
	filePath := filepath.Join(dir, "regular.txt")
	if err := os.WriteFile(filePath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	openRoot, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = openRoot.Close() }()
	fReal := &RootedFiles{root: openRoot}
	if _, err := fReal.readDir("regular.txt"); err == nil {
		t.Fatal("readDir on regular file want error")
	}

	// 6. mkdirParent rootHandle error (line 827)
	if err := f.mkdirParent("a/b"); err == nil {
		t.Fatal("mkdirParent rootHandle want error")
	}
}

func TestRootedFiles_OpenJSONLAppendFile_RetryExhausted(t *testing.T) {
	origOpen := openRootedFileSeam
	defer func() { openRootedFileSeam = origOpen }()
	openRootedFileSeam = func(root *os.Root, path string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, fs.ErrNotExist
	}

	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	f := &RootedFiles{root: root, fileOps: defaultRootedFileOps(), syncDirectory: syncRootDirectory}
	_, _, err = f.openJSONLAppendFile("a/b.jsonl")
	if err == nil || !strings.Contains(err.Error(), "directory chain remained unavailable") {
		t.Fatalf("openJSONLAppendFile want directory chain unavailable error, got %v", err)
	}
}

func TestRootedFiles_SyncRootDirectory_DirSyncError(t *testing.T) {
	origDirSync := dirSyncSeam
	defer func() { dirSyncSeam = origDirSync }()
	dirSyncSeam = func(f *os.File) error {
		return errors.New("dirSync fail")
	}

	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()

	if err := syncRootDirectory(root, "."); err == nil || !strings.Contains(err.Error(), "dirSync fail") {
		t.Fatalf("syncRootDirectory want dirSync fail, got %v", err)
	}
}

func TestRootedFiles_EnsureRealRootedScope_Gaps(t *testing.T) {
	dir := t.TempDir()

	// 1. root.Mkdir error (line 936)
	roDir := filepath.Join(dir, "ro_scope")
	if err := os.Mkdir(roDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(roDir, 0o755) }()
	roRoot, err := os.OpenRoot(roDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = roRoot.Close() }()
	if _, err := ensureRealRootedScope(roRoot, "sub"); err == nil {
		t.Fatal("ensureRealRootedScope in ro directory want error")
	}

	// 2. syncRootDirectorySeam error on "." (line 940)
	origSync := syncRootDirectorySeam
	defer func() { syncRootDirectorySeam = origSync }()
	syncRootDirectorySeam = func(root *os.Root, path string) error {
		if path == "." {
			return errors.New("parent sync fail")
		}
		return nil
	}
	normalRoot, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = normalRoot.Close() }()
	if _, err := ensureRealRootedScope(normalRoot, "scope1"); err == nil || !strings.Contains(err.Error(), "parent sync fail") {
		t.Fatalf("ensureRealRootedScope want parent sync fail, got %v", err)
	}

	// 3. syncRootDirectorySeam error on prefix (line 943)
	syncRootDirectorySeam = func(root *os.Root, path string) error {
		if path != "." {
			return errors.New("prefix sync fail")
		}
		return nil
	}
	if _, err := ensureRealRootedScope(normalRoot, "scope2"); err == nil || !strings.Contains(err.Error(), "prefix sync fail") {
		t.Fatalf("ensureRealRootedScope want prefix sync fail, got %v", err)
	}
	syncRootDirectorySeam = origSync

	// 4. rootLstatSeam error after Mkdir (line 949)
	origLstat := rootLstatSeam
	defer func() { rootLstatSeam = origLstat }()
	rootLstatSeam = func(root *os.Root, path string) (os.FileInfo, error) {
		return nil, errors.New("lstat after mkdir fail")
	}
	if _, err := ensureRealRootedScope(normalRoot, "scope3"); err == nil || !strings.Contains(err.Error(), "lstat after mkdir fail") {
		t.Fatalf("ensureRealRootedScope want lstat after mkdir fail, got %v", err)
	}
	rootLstatSeam = origLstat
}

func TestRootedFiles_JSONLReaders_DirectOps(t *testing.T) {
	// 1. readJSONLRecords seek error (line 1003)
	opsSeekFail := defaultRootedFileOps()
	opsSeekFail.seek = func(*os.File, int64, int) (int64, error) {
		return 0, errors.New("seek fail")
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tmpFile.Close() }()
	if _, err := readJSONLRecords(tmpFile, "p", opsSeekFail, 10); err == nil || !strings.Contains(err.Error(), "seek fail") {
		t.Fatalf("readJSONLRecords want seek fail, got %v", err)
	}

	// 2. readJSONLContent maxBytes == 0 (line 1032)
	if _, err := readJSONLContent(bytes.NewReader(nil), defaultRootedFileOps(), 0); err == nil {
		t.Fatal("readJSONLContent maxBytes 0 want error")
	}

	// 3. readJSONLContent n < 0 (line 1045)
	opsReadInvalid := defaultRootedFileOps()
	opsReadInvalid.read = func(io.Reader, []byte) (int, error) {
		return -1, nil
	}
	if _, err := readJSONLContent(bytes.NewReader([]byte("abc")), opsReadInvalid, 10); err == nil || !strings.Contains(err.Error(), "invalid JSONL reader byte count") {
		t.Fatalf("readJSONLContent want invalid byte count, got %v", err)
	}

	// 4. readJSONLContent read error (line 1052)
	opsReadErr := defaultRootedFileOps()
	opsReadErr.read = func(io.Reader, []byte) (int, error) {
		return 0, errors.New("read fail")
	}
	if _, err := readJSONLContent(bytes.NewReader([]byte("abc")), opsReadErr, 10); err == nil || !strings.Contains(err.Error(), "read fail") {
		t.Fatalf("readJSONLContent want read fail, got %v", err)
	}

	// 5. readJSONLContent n == 0 with nil error (line 1055)
	opsReadNoProg := defaultRootedFileOps()
	opsReadNoProg.read = func(io.Reader, []byte) (int, error) {
		return 0, nil
	}
	if _, err := readJSONLContent(bytes.NewReader([]byte("abc")), opsReadNoProg, 10); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("readJSONLContent want ErrNoProgress, got %v", err)
	}

	// 6. probeRootedJSONLOverflow n < 0 (line 1086)
	if _, err := probeRootedJSONLOverflow(bytes.NewReader(nil), opsReadInvalid, []byte("x")); err == nil || !strings.Contains(err.Error(), "invalid JSONL reader byte count") {
		t.Fatalf("probeRootedJSONLOverflow want invalid byte count, got %v", err)
	}

	// 7. probeRootedJSONLOverflow n == 0 with nil error (line 1092)
	if _, err := probeRootedJSONLOverflow(bytes.NewReader(nil), opsReadNoProg, []byte("x")); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("probeRootedJSONLOverflow want ErrNoProgress, got %v", err)
	}

	// 8. probeRootedJSONLOverflow non-EOF error (line 1095)
	if _, err := probeRootedJSONLOverflow(bytes.NewReader(nil), opsReadErr, []byte("x")); err == nil || !strings.Contains(err.Error(), "read fail") {
		t.Fatalf("probeRootedJSONLOverflow want read fail, got %v", err)
	}
}
