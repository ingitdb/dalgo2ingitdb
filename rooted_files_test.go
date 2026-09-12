//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package dalgo2ingitdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var incidentScope = RootedFilesScope{Prefix: "incidents"}

func openIncidentFiles(t *testing.T) (string, *RootedFiles) {
	t.Helper()
	root := t.TempDir()
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	files, err := RootedFilesFor(context.Background(), db, incidentScope)
	if err != nil {
		t.Fatalf("RootedFilesFor: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })
	return root, files
}

func TestRootedFilesAppendAndReadJSONL(t *testing.T) {
	t.Parallel()
	root, files := openIncidentFiles(t)
	const eventPath = "INC-1/events.jsonl"
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 1, "type": "created"}); err != nil {
		t.Fatalf("AppendJSONL first: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(root, "incidents", "INC-1", "events.jsonl"))
	if err != nil {
		t.Fatalf("read first event file: %v", err)
	}
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 2, "type": "note"}); err != nil {
		t.Fatalf("AppendJSONL second: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(root, "incidents", "INC-1", "events.jsonl"))
	if err != nil {
		t.Fatalf("read appended event file: %v", err)
	}
	if string(after[:len(before)]) != string(before) {
		t.Fatalf("AppendJSONL rewrote existing bytes: before=%q after=%q", before, after)
	}
	assertJSONLSeqs(t, files, eventPath, []int{1, 2})
}

func TestRootedFilesWriteAndReadJSON(t *testing.T) {
	t.Parallel()
	root, files := openIncidentFiles(t)
	const projectionPath = "INC-1/incident.json"
	want := struct {
		Title string `json:"title"`
		Seq   int    `json:"seq"`
	}{Title: "database unavailable", Seq: 2}
	if err := files.WriteJSONAtomic(projectionPath, want); err != nil {
		t.Fatalf("WriteJSONAtomic: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "incidents", "INC-1", "incident.json")); err != nil {
		t.Fatalf("projection has exact incident path: %v", err)
	}
	var got struct {
		Title string `json:"title"`
		Seq   int    `json:"seq"`
	}
	if err := files.ReadJSON(projectionPath, &got); err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	if got != want {
		t.Errorf("ReadJSON = %+v, want %+v", got, want)
	}
}

func TestRootedFilesScopedLockCanCallOtherOperations(t *testing.T) {
	_, files := openIncidentFiles(t)
	called := false
	if err := files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
		called = true
		return files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1})
	}); err != nil {
		t.Fatalf("WithExclusiveLock: %v", err)
	}
	if !called {
		t.Fatal("lock callback was not called")
	}
	assertJSONLSeqs(t, files, "INC-1/events.jsonl", []int{1})
	entries, err := files.ReadDir(".store")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "lock" {
		t.Fatalf("ReadDir entries = %v, want lock", entries)
	}
}

func TestRootedFilesWriteJSONAtomicWithMode(t *testing.T) {
	root, files := openIncidentFiles(t)
	if err := files.WriteJSONAtomicWithMode(".store/receipt.json", map[string]any{"kind": "receipt"}, 0o600); err != nil {
		t.Fatalf("WriteJSONAtomicWithMode: %v", err)
	}
	info, err := os.Stat(filepath.Join(root, "incidents", ".store", "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("receipt mode = %o, want 600", mode)
	}
	if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(filepath.Join(root, "incidents", "INC-1", "incident.json"))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o644 {
		t.Fatalf("projection mode = %o, want 644", mode)
	}
	if err := files.WriteJSONAtomicWithMode("invalid.json", map[string]any{}, os.ModeDir|0o600); err == nil {
		t.Fatal("non-permission mode accepted")
	}
}

func TestRootedFilesScopedLockHonorsContext(t *testing.T) {
	_, files := openIncidentFiles(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := files.WithExclusiveLock(ctx, ".store/lock", func() error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting lock error = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatalf("lock holder: %v", err)
	}
	if err := files.WithExclusiveLock(context.Background(), ".store/lock", nil); err == nil {
		t.Fatal("nil lock callback accepted")
	}
}

func TestRootedFilesInjectedFileOperationFailures(t *testing.T) {
	sentinel := errors.New("injected file failure")
	t.Run("append write", func(t *testing.T) {
		_, files := openIncidentFiles(t)
		files.fileOps.write = func(*os.File, []byte) (int, error) { return 0, sentinel }
		if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
			t.Fatalf("AppendJSONL write error = %v", err)
		}
	})
	t.Run("append sync", func(t *testing.T) {
		_, files := openIncidentFiles(t)
		files.fileOps.sync = func(*os.File) error { return sentinel }
		if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
			t.Fatalf("AppendJSONL sync error = %v", err)
		}
	})
	t.Run("read stream", func(t *testing.T) {
		_, files := openIncidentFiles(t)
		if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
			t.Fatal(err)
		}
		files.fileOps.readAll = func(io.Reader) ([]byte, error) { return nil, sentinel }
		if _, err := files.ReadJSONL("INC-1/events.jsonl"); !errors.Is(err, sentinel) {
			t.Fatalf("ReadJSONL read error = %v", err)
		}
	})
	t.Run("projection entropy", func(t *testing.T) {
		_, files := openIncidentFiles(t)
		files.fileOps.randomRead = func([]byte) (int, error) { return 0, sentinel }
		if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
			t.Fatalf("WriteJSONAtomic entropy error = %v", err)
		}
	})
	for _, tc := range []struct {
		name string
		set  func(*RootedFiles)
	}{
		{name: "projection write", set: func(files *RootedFiles) {
			files.fileOps.write = func(*os.File, []byte) (int, error) { return 0, sentinel }
		}},
		{name: "projection sync", set: func(files *RootedFiles) { files.fileOps.sync = func(*os.File) error { return sentinel } }},
		{name: "projection close", set: func(files *RootedFiles) {
			files.fileOps.close = func(file *os.File) error { _ = file.Close(); return sentinel }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, files := openIncidentFiles(t)
			tc.set(files)
			if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
				t.Fatalf("WriteJSONAtomic %s error = %v", tc.name, err)
			}
		})
	}
}

func TestRecoverJSONLTailInjectedFailures(t *testing.T) {
	sentinel := errors.New("injected recovery failure")
	openPartial := func(t *testing.T) *os.File {
		t.Helper()
		path := filepath.Join(t.TempDir(), "events.jsonl")
		if err := os.WriteFile(path, []byte(`{"seq":1`), 0o644); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		return file
	}
	for _, tc := range []struct {
		name string
		set  func(*rootedFileOps)
	}{
		{name: "seek start", set: func(ops *rootedFileOps) { ops.seek = func(*os.File, int64, int) (int64, error) { return 0, sentinel } }},
		{name: "read", set: func(ops *rootedFileOps) { ops.readAll = func(io.Reader) ([]byte, error) { return nil, sentinel } }},
		{name: "truncate", set: func(ops *rootedFileOps) { ops.truncate = func(*os.File, int64) error { return sentinel } }},
		{name: "recovery sync", set: func(ops *rootedFileOps) { ops.sync = func(*os.File) error { return sentinel } }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := defaultRootedFileOps()
			tc.set(&ops)
			if err := recoverJSONLTail(openPartial(t), "events.jsonl", ops); !errors.Is(err, sentinel) {
				t.Fatalf("recoverJSONLTail %s error = %v", tc.name, err)
			}
		})
	}
	t.Run("seek end", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "events.jsonl")
		if err := os.WriteFile(path, []byte("{\"seq\":1}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		ops := defaultRootedFileOps()
		ops.seek = func(_ *os.File, _ int64, whence int) (int64, error) {
			if whence == io.SeekEnd {
				return 0, sentinel
			}
			return file.Seek(0, io.SeekStart)
		}
		if err := recoverJSONLTail(file, "events.jsonl", ops); !errors.Is(err, sentinel) {
			t.Fatalf("recoverJSONLTail seek end error = %v", err)
		}
	})
}

func TestRootedFilesSyncsEveryNewDirectoryLink(t *testing.T) {
	_, files := openIncidentFiles(t)
	var got []string
	files.syncDirectory = func(root *os.Root, relativePath string) error {
		got = append(got, relativePath)
		return syncRootDirectory(root, relativePath)
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	want := []string{".", "INC-1", "INC-1"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("directory sync sequence = %q, want %q", got, want)
	}
}

func TestRootedFilesRecoversOnlyUnterminatedTail(t *testing.T) {
	root, files := openIncidentFiles(t)
	const eventPath = "INC-1/events.jsonl"
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	physical := filepath.Join(root, "incidents", "INC-1", "events.jsonl")
	if err := appendRaw(physical, `{"seq":2`); err != nil {
		t.Fatal(err)
	}
	if _, err := files.ReadJSONL(eventPath); err == nil {
		t.Fatal("ReadJSONL accepted incomplete tail")
	}
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 3}); err != nil {
		t.Fatalf("AppendJSONL recovery retry: %v", err)
	}
	raw, err := os.ReadFile(physical)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `{"seq":2`) {
		t.Fatalf("interrupted tail was retained: %q", raw)
	}
	assertJSONLSeqs(t, files, eventPath, []int{1, 3})
}

func TestRootedFilesRejectsMalformedCommittedTail(t *testing.T) {
	root, files := openIncidentFiles(t)
	const eventPath = "INC-1/events.jsonl"
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	physical := filepath.Join(root, "incidents", "INC-1", "events.jsonl")
	if err := appendRaw(physical, "not-json\n"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(physical)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.AppendJSONL(eventPath, map[string]any{"seq": 2}); err == nil {
		t.Fatal("AppendJSONL accepted malformed committed record")
	}
	after, err := os.ReadFile(physical)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("malformed committed bytes changed: before=%q after=%q", before, after)
	}
}

func TestRootedFilesRejectsEscapingPathsAndNestedSymlink(t *testing.T) {
	root, files := openIncidentFiles(t)
	for _, filePath := range []string{"../outside.jsonl", "/outside.jsonl", "INC-1/../../outside.jsonl"} {
		if err := files.AppendJSONL(filePath, map[string]any{"seq": 1}); err == nil {
			t.Errorf("AppendJSONL(%q): want escape refusal", filePath)
		}
	}
	if err := files.AppendJSONL("events.jsonl", []int{1}); err == nil {
		t.Error("AppendJSONL non-object: want error")
	}
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "incidents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "incidents", "INC-symlink")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := files.AppendJSONL("INC-symlink/events.jsonl", map[string]any{"seq": 1}); err == nil {
		t.Fatal("AppendJSONL followed nested symlink")
	}
	if _, err := os.Stat(filepath.Join(outside, "events.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside target was modified: %v", err)
	}
}

func TestRootedFilesRejectsInProjectSymlinkOutsideAuthorizedScope(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".ingitdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "incidents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../.ingitdb", filepath.Join(root, "incidents", "INC-1")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	files, err := RootedFilesFor(context.Background(), db, incidentScope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{name: "append", run: func() error { return files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}) }},
		{name: "projection", run: func() error { return files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}) }},
		{name: "event read", run: func() error { _, err := files.ReadJSONL("INC-1/events.jsonl"); return err }},
		{name: "projection read", run: func() error { return files.ReadJSON("INC-1/incident.json", &map[string]any{}) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); err == nil {
				t.Fatalf("%s followed in-project symlink outside scope", operation.name)
			}
		})
	}
	entries, err := os.ReadDir(filepath.Join(root, ".ingitdb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("outside authorized scope was modified: %v", entries)
	}
}

func TestRootedFilesRejectsScopeSwapDuringAcquisition(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "incidents"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".ingitdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := openRootedFilesWith(root, incidentScope, func(projectRoot *os.Root, prefix string) (*os.Root, error) {
		if err := os.Rename(filepath.Join(root, prefix), filepath.Join(root, "incidents-verified")); err != nil {
			return nil, err
		}
		if err := os.Symlink(".ingitdb", filepath.Join(root, prefix)); err != nil {
			return nil, err
		}
		return projectRoot.OpenRoot(prefix)
	})
	if err == nil {
		_ = files.Close()
		t.Fatal("scope acquisition accepted swapped in-project symlink")
	}
	entries, readErr := os.ReadDir(filepath.Join(root, ".ingitdb"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("scope acquisition mutated out-of-scope directory: %v", entries)
	}
}

func TestRootedFilesRejectsProjectRootSwapDuringAcquisition(t *testing.T) {
	parent := t.TempDir()
	project := filepath.Join(parent, "project")
	replacement := filepath.Join(parent, "replacement")
	if err := os.Mkdir(project, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replacement, 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := openRootedFilesWithProject(project, incidentScope, func(path string) (*os.Root, error) {
		if err := os.Rename(path, filepath.Join(parent, "project-verified")); err != nil {
			return nil, err
		}
		if err := os.Symlink(replacement, path); err != nil {
			return nil, err
		}
		return os.OpenRoot(path)
	}, func(root *os.Root, prefix string) (*os.Root, error) {
		return root.OpenRoot(prefix)
	})
	if err == nil {
		_ = files.Close()
		t.Fatal("project acquisition accepted swapped root")
	}
	for _, unchanged := range []string{filepath.Join(parent, "project-verified"), replacement} {
		entries, readErr := os.ReadDir(unchanged)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatalf("project acquisition mutated %q: %v", unchanged, entries)
		}
	}
}

func TestRootedFilesScopeRejectsMultiSegmentPrefix(t *testing.T) {
	multiSegment := RootedFilesScope{Prefix: "incidents/archive"}
	if _, err := multiSegment.validate(); err == nil {
		t.Fatal("multi-segment scope prefix accepted")
	}
	if _, err := NewDatabase(t.TempDir(), newReader(), WithRootedFilesScopes(multiSegment)); err == nil {
		t.Fatal("database accepted multi-segment rooted-files scope")
	}
}

func TestRootedFilesKeepsOpenedScopeAcrossScopePathSwap(t *testing.T) {
	root, files := openIncidentFiles(t)
	if err := os.Mkdir(filepath.Join(root, ".ingitdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(root, "incidents-verified")
	if err := os.Rename(filepath.Join(root, "incidents"), moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(".ingitdb", filepath.Join(root, "incidents")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "INC-1", "events.jsonl")); err != nil {
		t.Fatalf("opened scope did not receive event: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, "INC-1", "incident.json")); err != nil {
		t.Fatalf("opened scope did not receive projection: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".ingitdb", "INC-1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("swapped scope target was modified: %v", err)
	}
}

func TestRootedFilesRetrySyncsEventParentAfterPriorPostFileSyncFailure(t *testing.T) {
	root, files := openIncidentFiles(t)
	if err := os.Mkdir(filepath.Join(root, "incidents", "INC-retry"), 0o755); err != nil {
		t.Fatal(err)
	}
	var parentSyncs int
	postFileSyncErr := errors.New("simulated crash after event file sync")
	files.syncDirectory = func(root *os.Root, relativePath string) error {
		if relativePath == "INC-retry" {
			parentSyncs++
			if parentSyncs == 1 {
				return postFileSyncErr
			}
		}
		return syncRootDirectory(root, relativePath)
	}
	if err := files.AppendJSONL("INC-retry/events.jsonl", map[string]any{"seq": 1}); !errors.Is(err, postFileSyncErr) {
		t.Fatalf("first append = %v, want post-file-sync error", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "incidents", "INC-retry", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `{"seq":1}`) {
		t.Fatalf("event file was not synced before simulated crash: %q", raw)
	}
	if err := files.AppendJSONL("INC-retry/events.jsonl", map[string]any{"seq": 2}); err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if parentSyncs != 2 {
		t.Fatalf("event parent sync attempts = %d, want 2", parentSyncs)
	}
	assertJSONLSeqs(t, files, "INC-retry/events.jsonl", []int{1, 2})
}

func TestRootedFilesTwoCapabilitiesSerializeConcurrentAppends(t *testing.T) {
	root, first := openIncidentFiles(t)
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	second, err := RootedFilesFor(context.Background(), db, incidentScope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	const count = 64
	var wg sync.WaitGroup
	for i := range count {
		writer := first
		if i%2 == 1 {
			writer = second
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := writer.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": i}); err != nil {
				t.Errorf("AppendJSONL(%d): %v", i, err)
			}
		}()
	}
	wg.Wait()
	assertJSONLSeqsUnordered(t, first, "INC-1/events.jsonl", count)
}

func TestRootedFilesCloseWaitsForActiveOperation(t *testing.T) {
	_, files := openIncidentFiles(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	files.syncDirectory = func(root *os.Root, relativePath string) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return syncRootDirectory(root, relativePath)
	}
	appendDone := make(chan error, 1)
	go func() { appendDone <- files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}) }()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- files.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before operation finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("AppendJSONL: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 2}); err == nil {
		t.Fatal("AppendJSONL after Close: want error")
	}
	if _, err := files.ReadDir("."); err == nil {
		t.Fatal("ReadDir after Close: want error")
	}
	if err := files.WithExclusiveLock(context.Background(), ".store/lock", func() error { return nil }); err == nil {
		t.Fatal("WithExclusiveLock after Close: want error")
	}
}

func TestRootedFilesCloseWaitsForLockCallbackAndAllowsNestedOperations(t *testing.T) {
	_, files := openIncidentFiles(t)
	callbackEntered := make(chan struct{})
	startNested := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
			close(callbackEntered)
			<-startNested
			if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
				return err
			}
			if _, err := files.ReadJSONL("INC-1/events.jsonl"); err != nil {
				return err
			}
			if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); err != nil {
				return err
			}
			return files.ReadJSON("INC-1/incident.json", &map[string]any{})
		})
	}()
	<-callbackEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- files.Close() }()
	waitForRootedFilesCloseDrain(t, files)
	if err := files.WithExclusiveLock(context.Background(), ".store/second-lock", func() error { return nil }); err == nil {
		t.Fatal("new top-level lock was admitted while Close drained callback")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while lock callback was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(startNested)
	if err := <-lockDone; err != nil {
		t.Fatalf("lock callback nested operations: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRootedFilesCloseWaitsForLockWaitingOnFlock(t *testing.T) {
	_, files := openIncidentFiles(t)
	holderEntered := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
			close(holderEntered)
			<-releaseHolder
			return nil
		})
	}()
	<-holderEntered
	waiterEntered := make(chan struct{})
	releaseWaiter := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
			close(waiterEntered)
			if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
				return err
			}
			<-releaseWaiter
			return nil
		})
	}()
	select {
	case <-waiterEntered:
		t.Fatal("second lock callback entered before the first released flock")
	case <-time.After(20 * time.Millisecond):
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- files.Close() }()
	waitForRootedFilesCloseDrain(t, files)
	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("lock holder: %v", err)
	}
	<-waiterEntered
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while second callback was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWaiter)
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiting lock callback: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRootedFilesExclusiveLockClosesFileWhenInitialParentSyncFails(t *testing.T) {
	root, files := openIncidentFiles(t)
	if err := os.Mkdir(filepath.Join(root, "incidents", ".store"), 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("injected lock parent sync failure")
	var closes int
	files.fileOps.close = func(file *os.File) error {
		closes++
		return file.Close()
	}
	files.syncDirectory = func(root *os.Root, relativePath string) error {
		if relativePath == ".store" {
			return sentinel
		}
		return syncRootDirectory(root, relativePath)
	}
	if err := files.WithExclusiveLock(context.Background(), ".store/lock", func() error {
		t.Fatal("lock callback ran after initial parent sync failure")
		return nil
	}); !errors.Is(err, sentinel) {
		t.Fatalf("WithExclusiveLock error = %v, want injected sync error", err)
	}
	if closes != 1 {
		t.Fatalf("lock file close calls = %d, want 1", closes)
	}
}

func TestRootedFilesOperationFailurePaths(t *testing.T) {
	_, files := openIncidentFiles(t)
	if _, err := files.ReadDir("missing"); err == nil {
		t.Fatal("ReadDir accepted missing directory")
	}
	if _, err := files.ReadDir("../escape"); err == nil {
		t.Fatal("ReadDir accepted escaping path")
	}
	if err := files.ReadJSON("missing.json", &map[string]any{}); err == nil {
		t.Fatal("ReadJSON accepted missing document")
	}
	if err := files.WriteJSONAtomic("invalid-target.json", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	if err := files.ReadJSON("invalid-target.json", nil); err == nil {
		t.Fatal("ReadJSON accepted invalid decode target")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := files.WithExclusiveLock(cancelled, ".store/lock", func() error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled WithExclusiveLock = %v, want context canceled", err)
	}
	if err := files.WithExclusiveLock(context.Background(), "../escape", func() error { return nil }); err == nil {
		t.Fatal("WithExclusiveLock accepted escaping path")
	}
	sentinel := errors.New("injected mkdir sync failure")
	files.syncDirectory = func(*os.Root, string) error { return sentinel }
	if err := files.WriteJSONAtomic("new/incident.json", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
		t.Fatalf("WriteJSONAtomic mkdir sync error = %v", err)
	}
}

func TestRootedFilesPrivateClosedFailurePaths(t *testing.T) {
	if _, err := (&RootedFiles{}).openFile("file", os.O_RDONLY, 0); err == nil {
		t.Fatal("openFile accepted absent root")
	}
	var nilFiles *RootedFiles
	if err := nilFiles.beginOperation(true); err == nil {
		t.Fatal("nil RootedFiles accepted operation")
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syncRootDirectory(root, "."); err == nil {
		t.Fatal("syncRootDirectory accepted closed root")
	}
}

func waitForRootedFilesCloseDrain(t *testing.T, files *RootedFiles) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		files.lifecycleMu.Lock()
		closing := files.closing
		files.lifecycleMu.Unlock()
		if closing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Close did not enter lifecycle draining state")
}

func TestRootedFilesDALgoAuthorizationBoundary(t *testing.T) {
	root := t.TempDir()
	plain, err := NewDatabase(root, newReader())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RootedFilesFor(context.Background(), plain, incidentScope); err == nil {
		t.Fatal("unconfigured database exposed rooted files")
	}
	configured, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RootedFilesFor(context.Background(), configured, RootedFilesScope{Prefix: "other"}); err == nil {
		t.Fatal("unconfigured scope was accepted")
	}
	protected, err := NewDatabase(root, newReader(), WithProtectedProfile(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RootedFilesFor(context.Background(), protected, incidentScope); err == nil {
		t.Fatal("protected facade exposed raw rooted files")
	}
	accessDir := filepath.Join(root, ".ingitdb", "access")
	if err := os.MkdirAll(accessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(accessDir, "manifest.yaml"), []byte("enabled: true\ndatabase: rooted-files-test\npolicies: [deny.yaml]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := `apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: deny-all}
target: {database: rooted-files-test}
composition: dalgo-hierarchical-v1
default: deny
scopes: []
`
	if err := os.WriteFile(filepath.Join(accessDir, "deny.yaml"), []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	secured, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatalf("NewDatabase secured: %v", err)
	}
	if _, err := RootedFilesFor(context.Background(), secured, incidentScope); err == nil {
		t.Fatal("secured facade exposed raw rooted files")
	}
}

func TestRootedFilesKeepsOpenedRootAcrossPathSwap(t *testing.T) {
	rootParent := t.TempDir()
	root := filepath.Join(rootParent, "repository")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	files, err := RootedFilesFor(context.Background(), db, incidentScope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	outside := filepath.Join(rootParent, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(rootParent, "repository-moved")
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "incidents", "INC-1", "events.jsonl")); err != nil {
		t.Fatalf("opened root did not receive event: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, "incidents", "INC-1", "incident.json")); err != nil {
		t.Fatalf("opened root did not receive projection: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "incidents")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("swapped symlink target changed: %v", err)
	}
}

func TestRootedFilesFailureSemantics(t *testing.T) {
	plain, err := NewDatabase(t.TempDir(), newReader())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapRootedFilesDatabase(plain, nil, []RootedFilesScope{{Prefix: "../escape"}}); err == nil {
		t.Fatal("escaping configured scope accepted")
	}
	if _, err := (&rootedFilesDatabase{DB: plain, scopes: map[string]struct{}{"incidents": {}}}).OpenRootedFiles(context.Background(), incidentScope); err == nil {
		t.Fatal("missing rooted-files backend accepted")
	}
	if _, err := openRootedFiles("", incidentScope); err == nil {
		t.Fatal("empty root accepted")
	}
	if _, err := openRootedFiles(t.TempDir(), RootedFilesScope{Prefix: "../escape"}); err == nil {
		t.Fatal("escaping scope accepted")
	}
	if _, err := openRootedFiles(filepath.Join(t.TempDir(), "missing"), incidentScope); err == nil {
		t.Fatal("missing root accepted")
	}
	fileRoot := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openRootedFiles(fileRoot, incidentScope); err == nil {
		t.Fatal("file root accepted")
	}
	rootForOpenErrors := t.TempDir()
	if _, err := openRootedFilesWithProject(rootForOpenErrors, incidentScope, func(string) (*os.Root, error) {
		return nil, errors.New("injected project open failure")
	}, func(root *os.Root, prefix string) (*os.Root, error) { return root.OpenRoot(prefix) }); err == nil {
		t.Fatal("project open failure accepted")
	}
	if _, err := openRootedFilesWithProject(rootForOpenErrors, incidentScope, func(path string) (*os.Root, error) {
		root, err := os.OpenRoot(path)
		if err != nil {
			return nil, err
		}
		_ = root.Close()
		return root, nil
	}, func(root *os.Root, prefix string) (*os.Root, error) { return root.OpenRoot(prefix) }); err == nil {
		t.Fatal("closed project root accepted")
	}
	if err := os.WriteFile(filepath.Join(rootForOpenErrors, "incidents"), []byte("not directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openRootedFiles(rootForOpenErrors, incidentScope); err == nil {
		t.Fatal("file scope accepted")
	}
	if err := os.Remove(filepath.Join(rootForOpenErrors, "incidents")); err != nil {
		t.Fatal(err)
	}
	if _, err := openRootedFilesWith(rootForOpenErrors, incidentScope, func(*os.Root, string) (*os.Root, error) {
		return nil, errors.New("injected scope open failure")
	}); err == nil {
		t.Fatal("scope open failure accepted")
	}
	if _, err := openRootedFilesWith(rootForOpenErrors, incidentScope, func(root *os.Root, prefix string) (*os.Root, error) {
		scope, err := root.OpenRoot(prefix)
		if err != nil {
			return nil, err
		}
		_ = scope.Close()
		return scope, nil
	}); err == nil {
		t.Fatal("closed scope root accepted")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(t.TempDir(), link); err == nil {
		if _, err := openRootedFiles(link, incidentScope); err == nil {
			t.Fatal("symlink root accepted")
		}
	}
	if err := (*RootedFiles)(nil).Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	root, files := openIncidentFiles(t)
	if _, err := RootedFilesFor(context.Background(), nil, incidentScope); err == nil {
		t.Fatal("nil database accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RootedFilesFor(ctx, db, incidentScope); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled scope request = %v, want context cancellation", err)
	}
	if _, err := RootedFilesFor(context.Background(), db, RootedFilesScope{Prefix: "../escape"}); err == nil {
		t.Fatal("invalid requested scope accepted")
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", make(chan int)); err == nil {
		t.Fatal("unmarshalable JSONL value accepted")
	}
	if err := files.WriteJSONAtomic("INC-1/incident.json", make(chan int)); err == nil {
		t.Fatal("unmarshalable projection accepted")
	}
	if err := files.WriteJSONAtomic("../outside.json", map[string]any{}); err == nil {
		t.Fatal("escaping projection accepted")
	}
	if _, err := files.ReadJSONL("../outside.jsonl"); err == nil {
		t.Fatal("escaping JSONL read accepted")
	}
	if err := files.ReadJSON("../outside.json", &map[string]any{}); err == nil {
		t.Fatal("escaping projection read accepted")
	}
	if err := files.ReadJSON("missing.json", &map[string]any{}); err == nil {
		t.Fatal("missing projection accepted")
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if err := files.AppendJSONL("INC-1/events.jsonl", map[string]any{"seq": 1}); err == nil {
		t.Fatal("closed capability appended")
	}
	if _, err := files.ReadJSONL("INC-1/events.jsonl"); err == nil {
		t.Fatal("closed capability read")
	}
	if err := files.WriteJSONAtomic("INC-1/incident.json", map[string]any{"seq": 1}); err == nil {
		t.Fatal("closed capability wrote projection")
	}
	if err := files.ReadJSON("INC-1/incident.json", &map[string]any{}); err == nil {
		t.Fatal("closed capability read projection")
	}
}

func TestRootedFilesReadAndSyncFailures(t *testing.T) {
	root, files := openIncidentFiles(t)
	sentinel := errors.New("directory sync failed")
	physical := filepath.Join(root, "incidents", "INC-1")
	if err := os.MkdirAll(physical, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(physical, "incident.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := files.ReadJSON("INC-1/incident.json", &map[string]any{}); err == nil {
		t.Fatal("invalid projection JSON accepted")
	}
	if err := os.WriteFile(filepath.Join(physical, "events.jsonl"), []byte("not-json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := files.ReadJSONL("INC-1/events.jsonl"); err == nil {
		t.Fatal("invalid committed JSONL accepted")
	}
	if err := os.Mkdir(filepath.Join(physical, "stream-dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := files.ReadJSONL("INC-1/stream-dir"); err == nil {
		t.Fatal("directory accepted as JSONL stream")
	}
	if err := os.Mkdir(filepath.Join(physical, "directory.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteJSONAtomic("INC-1/directory.json", map[string]any{"seq": 1}); err == nil {
		t.Fatal("projection replaced a directory")
	}
	if err := os.Mkdir(filepath.Join(physical, "directory.jsonl"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := files.openJSONLAppendFile("INC-1/directory.jsonl"); err == nil {
		t.Fatal("JSONL append opened a directory")
	}
	if err := files.mkdirParent("file.json"); err != nil {
		t.Fatalf("root-level parent: %v", err)
	}
	blockingRoot, blockingFiles := openIncidentFiles(t)
	if err := os.WriteFile(filepath.Join(blockingRoot, "incidents", "blocked"), []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := blockingFiles.mkdirParent("blocked/INC-1/events.jsonl"); err == nil {
		t.Fatal("file accepted as a parent directory")
	}
	childSyncErr := errors.New("child directory sync failed")
	files.syncDirectory = func(root *os.Root, relativePath string) error {
		if relativePath == "INC-child" {
			return childSyncErr
		}
		return syncRootDirectory(root, relativePath)
	}
	if err := files.mkdirParent("INC-child/events.jsonl"); !errors.Is(err, childSyncErr) {
		t.Fatalf("child directory sync error = %v, want %v", err, childSyncErr)
	}
	files.syncDirectory = syncRootDirectory
	if err := os.MkdirAll(filepath.Join(root, "incidents", "INC-leaf"), 0o755); err != nil {
		t.Fatal(err)
	}
	files.syncDirectory = func(*os.Root, string) error { return sentinel }
	if err := files.AppendJSONL("INC-leaf/events.jsonl", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
		t.Fatalf("AppendJSONL leaf sync error = %v, want %v", err, sentinel)
	}
	files.syncDirectory = func(*os.Root, string) error { return sentinel }
	if err := files.AppendJSONL("INC-2/events.jsonl", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
		t.Fatalf("AppendJSONL directory sync error = %v, want %v", err, sentinel)
	}
	if err := files.WriteJSONAtomic("INC-1/other.json", map[string]any{"seq": 1}); !errors.Is(err, sentinel) {
		t.Fatalf("WriteJSONAtomic directory sync error = %v, want %v", err, sentinel)
	}
	files.syncDirectory = nil
	if err := files.syncDir("incidents"); err == nil {
		t.Fatal("nil directory sync hook accepted")
	}
	if err := (&RootedFiles{}).syncDir("anything"); err == nil {
		t.Fatal("closed directory sync accepted")
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rootHandle.Close() }()
	if err := syncRootDirectory(rootHandle, "missing"); err == nil {
		t.Fatal("missing directory sync accepted")
	}
}

func TestRecoverJSONLTailSemantics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	for _, tc := range []struct {
		name    string
		content string
		want    string
		wantErr bool
	}{
		{name: "empty"},
		{name: "complete", content: "{\"seq\":1}\n", want: "{\"seq\":1}\n"},
		{name: "partial", content: "{\"seq\":1}\n{\"seq\":2", want: "{\"seq\":1}\n"},
		{name: "malformed committed", content: "nope\n", want: "nope\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			err = recoverJSONLTail(file, "incidents/INC-1/events.jsonl", defaultRootedFileOps())
			_ = file.Close()
			if (err != nil) != tc.wantErr {
				t.Fatalf("recoverJSONLTail error = %v, wantErr %v", err, tc.wantErr)
			}
			got, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(got) != tc.want {
				t.Fatalf("recovered bytes = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRecoverJSONLTailFailsOnReadOnlyTruncateAndClosedDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"seq":1`), 0o644); err != nil {
		t.Fatal(err)
	}
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := recoverJSONLTail(readOnly, "events.jsonl", defaultRootedFileOps()); err == nil {
		t.Fatal("read-only recovery truncated")
	}
	_ = readOnly.Close()
	closed, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if err := recoverJSONLTail(closed, "events.jsonl", defaultRootedFileOps()); err == nil {
		t.Fatal("closed descriptor recovered")
	}
}

func TestRootedFileLocksRejectClosedDescriptor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "lock")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if err := withRootedSharedFileLock(file, func() error { return nil }); err == nil {
		t.Fatal("shared lock accepted closed descriptor")
	}
	if err := withRootedExclusiveFileLock(file, func() error { return nil }); err == nil {
		t.Fatal("exclusive lock accepted closed descriptor")
	}
}

func appendRaw(path, content string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteString(content); err != nil {
		return err
	}
	return file.Sync()
}

func assertJSONLSeqs(t *testing.T, files *RootedFiles, eventPath string, want []int) {
	t.Helper()
	records, err := files.ReadJSONL(eventPath)
	if err != nil {
		t.Fatalf("ReadJSONL: %v", err)
	}
	if len(records) != len(want) {
		t.Fatalf("ReadJSONL record count = %d, want %d", len(records), len(want))
	}
	for i, sequence := range want {
		var record struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(records[i], &record); err != nil {
			t.Fatalf("decode record %d: %v", i, err)
		}
		if record.Seq != sequence {
			t.Errorf("record %d seq = %d, want %d", i, record.Seq, sequence)
		}
	}
}

func assertJSONLSeqsUnordered(t *testing.T, files *RootedFiles, eventPath string, count int) {
	t.Helper()
	records, err := files.ReadJSONL(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("record count = %d, want %d", len(records), count)
	}
	seen := make(map[int]bool, count)
	for _, raw := range records {
		var record struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		seen[record.Seq] = true
	}
	for i := range count {
		if !seen[i] {
			t.Errorf("missing event %d", i)
		}
	}
}
