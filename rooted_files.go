package dalgo2ingitdb

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dal-go/dalgo/dal"
)

// RootedFiles is a small, descriptor-rooted file capability exposed by the
// inGitDB DALgo adapter. It is for data formats whose persistence semantics
// cannot be represented by DALgo's keyed-record API, such as an append-only
// JSONL event stream.
//
// Every path is relative to the opened project root. RootedFiles rejects an
// escaping path before handing it to os.Root, and os.Root keeps the opened
// directory stable when the original root pathname is renamed or replaced.
// Call Close when the capability is no longer needed.
type RootedFiles struct {
	root *os.Root
	mu   sync.Mutex
}

// RootedFilesProvider is an optional DALgo capability offered by Database.
// Obtain it with RootedFilesFor instead of assuming a concrete database type.
type RootedFilesProvider interface {
	OpenRootedFiles() (*RootedFiles, error)
}

// OpenRootedFiles opens a descriptor-rooted file capability at projectPath.
// The root itself must be a real directory, not a symlink.
func OpenRootedFiles(projectPath string) (*RootedFiles, error) {
	if strings.TrimSpace(projectPath) == "" {
		return nil, errors.New("dalgo2ingitdb: rooted files project path is required")
	}
	abs, err := filepath.Abs(projectPath)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: absolute rooted files path: %w", err)
	}
	abs = filepath.Clean(abs)
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files root must be a real directory")
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open rooted files root: %w", err)
	}
	return &RootedFiles{root: root}, nil
}

// RootedFilesFor opens RootedFiles from a DALgo database that advertises the
// optional RootedFilesProvider capability.
func RootedFilesFor(db dal.DB) (*RootedFiles, error) {
	if db == nil {
		return nil, errors.New("dalgo2ingitdb: rooted files database is required")
	}
	provider, ok := dal.As[RootedFilesProvider](db)
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: database %q does not provide rooted files", db.ID())
	}
	return provider.OpenRootedFiles()
}

// OpenRootedFiles exposes RootedFiles as a DALgo optional capability.
func (db *Database) OpenRootedFiles() (*RootedFiles, error) {
	return OpenRootedFiles(db.projectPath)
}

// Close releases the opened root directory handle.
func (f *RootedFiles) Close() error {
	if f == nil || f.root == nil {
		return nil
	}
	err := f.root.Close()
	f.root = nil
	return err
}

// AppendJSONL serializes value as one JSON object and durably appends it to
// relativePath. Existing bytes are never reserialized or rewritten. On
// platforms with file locking, cooperating RootedFiles readers and writers are
// serialized around the complete line.
func (f *RootedFiles) AppendJSONL(relativePath string, value any) error {
	relativePath, err := rootedRelativePath(relativePath)
	if err != nil {
		return err
	}
	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: encode JSONL record: %w", err)
	}
	if len(content) == 0 || content[0] != '{' {
		return errors.New("dalgo2ingitdb: JSONL record must be a JSON object")
	}
	content = append(content, '\n')
	// os.Root.MkdirAll is safe for one caller, but two first writers can still
	// race between creating the nested parent and opening the leaf. Keep that
	// setup atomic for one RootedFiles handle; the file descriptor lock below
	// continues to coordinate append publication.
	f.mu.Lock()
	if err := f.mkdirParent(relativePath); err != nil {
		f.mu.Unlock()
		return err
	}
	file, err := f.openFile(relativePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	f.mu.Unlock()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: open JSONL append %q: %w", relativePath, err)
	}
	defer func() { _ = file.Close() }()
	return withRootedExclusiveFileLock(file, func() error {
		if _, err := file.Write(content); err != nil {
			return fmt.Errorf("dalgo2ingitdb: append JSONL %q: %w", relativePath, err)
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("dalgo2ingitdb: sync JSONL %q: %w", relativePath, err)
		}
		return nil
	})
}

// ReadJSONL returns each non-blank JSON object from relativePath in file
// order. The returned values are independent copies of the on-disk lines.
func (f *RootedFiles) ReadJSONL(relativePath string) ([]json.RawMessage, error) {
	relativePath, err := rootedRelativePath(relativePath)
	if err != nil {
		return nil, err
	}
	file, err := f.openFile(relativePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open JSONL read %q: %w", relativePath, err)
	}
	defer func() { _ = file.Close() }()
	var records []json.RawMessage
	err = withRootedSharedFileLock(file, func() error {
		content, readErr := io.ReadAll(file)
		if readErr != nil {
			return fmt.Errorf("dalgo2ingitdb: read JSONL %q: %w", relativePath, readErr)
		}
		for lineNumber, raw := range bytes.Split(content, []byte{'\n'}) {
			line := bytes.TrimSpace(raw)
			if len(line) == 0 {
				continue
			}
			if !json.Valid(line) || line[0] != '{' {
				return fmt.Errorf("dalgo2ingitdb: invalid JSONL record at %q line %d", relativePath, lineNumber+1)
			}
			records = append(records, append(json.RawMessage(nil), line...))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// WriteJSONAtomic serializes value and atomically publishes it at
// relativePath. It writes and syncs a private sibling temporary file before
// rename, then syncs the containing directory so a crash cannot expose a
// partially-written projection. It does not choose a winner between concurrent
// projection writers; callers that need a sequence contract must serialize at
// their mutation boundary.
func (f *RootedFiles) WriteJSONAtomic(relativePath string, value any) (err error) {
	relativePath, err = rootedRelativePath(relativePath)
	if err != nil {
		return err
	}
	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: encode JSON %q: %w", relativePath, err)
	}
	content = append(content, '\n')
	if err := f.mkdirParent(relativePath); err != nil {
		return err
	}
	root, err := f.rootHandle()
	if err != nil {
		return err
	}
	dir, base := path.Dir(relativePath), path.Base(relativePath)
	tempPath, err := rootedTemporaryPath(dir, base)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = root.Remove(tempPath)
		}
	}()
	file, err := f.openFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: create JSON temporary file %q: %w", tempPath, err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("dalgo2ingitdb: write JSON temporary file %q: %w", tempPath, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("dalgo2ingitdb: sync JSON temporary file %q: %w", tempPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("dalgo2ingitdb: close JSON temporary file %q: %w", tempPath, err)
	}
	if err := root.Rename(tempPath, relativePath); err != nil {
		return fmt.Errorf("dalgo2ingitdb: publish JSON %q: %w", relativePath, err)
	}
	if err := f.syncDir(dir); err != nil {
		return err
	}
	return nil
}

// ReadJSON decodes the JSON document at relativePath into target.
func (f *RootedFiles) ReadJSON(relativePath string, target any) error {
	relativePath, err := rootedRelativePath(relativePath)
	if err != nil {
		return err
	}
	root, err := f.rootHandle()
	if err != nil {
		return err
	}
	content, err := root.ReadFile(relativePath)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: read JSON %q: %w", relativePath, err)
	}
	if err := json.Unmarshal(content, target); err != nil {
		return fmt.Errorf("dalgo2ingitdb: decode JSON %q: %w", relativePath, err)
	}
	return nil
}

func (f *RootedFiles) openFile(relativePath string, flag int, perm os.FileMode) (*os.File, error) {
	root, err := f.rootHandle()
	if err != nil {
		return nil, err
	}
	return root.OpenFile(relativePath, flag, perm)
}

func (f *RootedFiles) mkdirParent(relativePath string) error {
	dir := path.Dir(relativePath)
	if dir == "." {
		return nil
	}
	root, err := f.rootHandle()
	if err != nil {
		return err
	}
	if err := root.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("dalgo2ingitdb: create rooted directory %q: %w", dir, err)
	}
	return nil
}

func (f *RootedFiles) syncDir(relativePath string) error {
	root, err := f.rootHandle()
	if err != nil {
		return err
	}
	dir, err := root.Open(relativePath)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: open JSON directory %q: %w", relativePath, err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("dalgo2ingitdb: sync JSON directory %q: %w", relativePath, err)
	}
	return nil
}

func (f *RootedFiles) rootHandle() (*os.Root, error) {
	if f == nil || f.root == nil {
		return nil, errors.New("dalgo2ingitdb: rooted files is closed")
	}
	return f.root, nil
}

func rootedRelativePath(value string) (string, error) {
	if value == "" || strings.Contains(value, `\`) || path.IsAbs(value) {
		return "", fmt.Errorf("dalgo2ingitdb: rooted file path %q must be non-empty, relative, and slash-separated", value)
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("dalgo2ingitdb: rooted file path %q escapes root", value)
	}
	return clean, nil
}

func rootedTemporaryPath(dir, base string) (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: generate temporary JSON file suffix: %w", err)
	}
	return path.Join(dir, "."+base+".tmp-"+hex.EncodeToString(random[:])), nil
}
