package dalgo2ingitdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
// Every path is relative to the authorized scope, which is itself the opened
// descriptor root. RootedFiles rejects an escaping path before handing it to
// os.Root, and os.Root keeps the opened directory stable when the original
// root pathname is renamed or replaced. Call Close when the capability is no
// longer needed.
type RootedFiles struct {
	root       *os.Root
	scope      RootedFilesScope
	mu         sync.RWMutex
	creationMu sync.Mutex
	// lifecycleMu deliberately does not guard root. It tracks outstanding
	// operations so Close can first drain them and only then take mu to close
	// the descriptor. Keeping that wait separate avoids RWMutex writer
	// preference deadlocking a lock callback which re-enters this capability.
	lifecycleMu   sync.Mutex
	lifecycleCond *sync.Cond
	closing       bool
	closed        bool
	operations    int
	callbacks     int
	syncDirectory func(*os.Root, string) error
	fileOps       rootedFileOps
}

// rootedFileOps is deliberately private and per capability. It gives focused
// tests deterministic access to OS/entropy failure paths without changing the
// public API or relying on process-global test hooks.
type rootedFileOps struct {
	write      func(*os.File, []byte) (int, error)
	sync       func(*os.File) error
	close      func(*os.File) error
	readAll    func(io.Reader) ([]byte, error)
	seek       func(*os.File, int64, int) (int64, error)
	truncate   func(*os.File, int64) error
	randomRead func([]byte) (int, error)
}

func defaultRootedFileOps() rootedFileOps {
	return rootedFileOps{
		write:      func(file *os.File, value []byte) (int, error) { return file.Write(value) },
		sync:       func(file *os.File) error { return file.Sync() },
		close:      func(file *os.File) error { return file.Close() },
		readAll:    io.ReadAll,
		seek:       func(file *os.File, offset int64, whence int) (int64, error) { return file.Seek(offset, whence) },
		truncate:   func(file *os.File, size int64) error { return file.Truncate(size) },
		randomRead: rand.Read,
	}
}

// RootedFilesScope is an explicitly authorized, top-level directory for raw
// inGitDB files. The MVP intentionally supports exactly one clean path segment
// (for example, "incidents"), configured by a trusted server mount rather than
// accepted from an untrusted request.
type RootedFilesScope struct {
	Prefix string
}

func (s RootedFilesScope) validate() (string, error) {
	prefix, err := rootedRelativePath(s.Prefix)
	if err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: rooted files scope: %w", err)
	}
	if strings.Contains(prefix, "/") {
		return "", fmt.Errorf("dalgo2ingitdb: rooted files scope %q must be one top-level path segment", s.Prefix)
	}
	return prefix, nil
}

// RootedFilesProvider is an optional DALgo capability intentionally exposed
// only by an unprotected server mount configured with WithRootedFilesScopes.
// Protected and secured facades do not forward it because their record ACLs do
// not authorize arbitrary raw-file paths.
type RootedFilesProvider interface {
	OpenRootedFiles(ctx context.Context, scope RootedFilesScope) (*RootedFiles, error)
}

type rootedFilesDatabase struct {
	dal.DB
	backend *Database
	scopes  map[string]struct{}
}

func wrapRootedFilesDatabase(db dal.DB, backend *Database, scopes []RootedFilesScope) (dal.DB, error) {
	if len(scopes) == 0 {
		return db, nil
	}
	allowed := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		prefix, err := scope.validate()
		if err != nil {
			return nil, err
		}
		allowed[prefix] = struct{}{}
	}
	return &rootedFilesDatabase{DB: db, backend: backend, scopes: allowed}, nil
}

func (db *rootedFilesDatabase) OpenRootedFiles(ctx context.Context, scope RootedFilesScope) (*RootedFiles, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix, err := scope.validate()
	if err != nil {
		return nil, err
	}
	if _, ok := db.scopes[prefix]; !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files scope %q is not authorized", prefix)
	}
	if db.backend == nil {
		return nil, errors.New("dalgo2ingitdb: rooted files backend unavailable")
	}
	return openRootedFiles(db.backend.projectPath, RootedFilesScope{Prefix: prefix})
}

// openRootedFilesWith keeps the scope-open operation injectable for a
// deterministic check of the verify/open swap boundary.
func openRootedFilesWith(projectPath string, scope RootedFilesScope, openScope func(*os.Root, string) (*os.Root, error)) (*RootedFiles, error) {
	return openRootedFilesWithProject(projectPath, scope, os.OpenRoot, openScope)
}

// openRootedFilesWithProject keeps the project-root open operation injectable
// for a deterministic check of the project verify/open swap boundary.
func openRootedFilesWithProject(projectPath string, scope RootedFilesScope, openProject func(string) (*os.Root, error), openScope func(*os.Root, string) (*os.Root, error)) (*RootedFiles, error) {
	prefix, err := scope.validate()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(projectPath) == "" {
		return nil, errors.New("dalgo2ingitdb: rooted files project path is required")
	}
	abs, err := filepath.Abs(projectPath)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: absolute rooted files path: %w", err)
	}
	abs = filepath.Clean(abs)
	verifiedProject, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files root: %w", err)
	}
	if verifiedProject.Mode()&os.ModeSymlink != 0 || !verifiedProject.IsDir() {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files root must be a real directory")
	}
	projectRoot, err := openProject(abs)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open rooted files root: %w", err)
	}
	defer func() { _ = projectRoot.Close() }()
	openedProject, err := projectRoot.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: stat opened rooted files root: %w", err)
	}
	if !os.SameFile(verifiedProject, openedProject) {
		return nil, errors.New("dalgo2ingitdb: rooted files root changed during acquisition")
	}
	verifiedScope, err := ensureRealRootedScope(projectRoot, prefix)
	if err != nil {
		return nil, err
	}
	root, err := openScope(projectRoot, prefix)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open rooted files scope %q: %w", prefix, err)
	}
	openedScope, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("dalgo2ingitdb: stat opened rooted files scope %q: %w", prefix, err)
	}
	if !os.SameFile(verifiedScope, openedScope) {
		_ = root.Close()
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files scope %q changed during acquisition", prefix)
	}
	return &RootedFiles{root: root, scope: RootedFilesScope{Prefix: prefix}, syncDirectory: syncRootDirectory, fileOps: defaultRootedFileOps()}, nil
}

// RootedFilesFor opens a scoped rooted-file capability from a DALgo database
// that explicitly advertises RootedFilesProvider. It deliberately uses a
// direct assertion rather than dal.As so a secured or protected wrapper cannot
// unwrap and bypass its ACL boundary.
func RootedFilesFor(ctx context.Context, db dal.DB, scope RootedFilesScope) (*RootedFiles, error) {
	if db == nil {
		return nil, errors.New("dalgo2ingitdb: rooted files database is required")
	}
	provider, ok := db.(RootedFilesProvider)
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: database %q does not provide authorized rooted files", db.ID())
	}
	return provider.OpenRootedFiles(ctx, scope)
}

// Close releases the opened root directory handle.
func (f *RootedFiles) Close() error {
	if f == nil {
		return nil
	}
	f.lifecycleMu.Lock()
	f.closing = true
	cond := f.lifecycleConditionLocked()
	for f.operations > 0 {
		cond.Wait()
	}
	f.closed = true
	f.lifecycleMu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.root == nil {
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
	if err := f.beginOperation(true); err != nil {
		return err
	}
	defer f.endOperation()
	f.mu.RLock()
	defer f.mu.RUnlock()
	relativePath, err := f.scopedRelativePath(relativePath)
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
	f.creationMu.Lock()
	file, _, err := f.openJSONLAppendFile(relativePath)
	f.creationMu.Unlock()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: open JSONL append %q: %w", relativePath, err)
	}
	defer func() { _ = f.fileOps.close(file) }()
	return withRootedExclusiveFileLock(file, func() error {
		if err := recoverJSONLTail(file, relativePath, f.fileOps); err != nil {
			return err
		}
		if _, err := f.fileOps.write(file, content); err != nil {
			return fmt.Errorf("dalgo2ingitdb: append JSONL %q: %w", relativePath, err)
		}
		if err := f.fileOps.sync(file); err != nil {
			return fmt.Errorf("dalgo2ingitdb: sync JSONL %q: %w", relativePath, err)
		}
		// Even when an earlier writer created the file, a crash after that
		// writer's file sync but before its directory sync leaves the parent
		// entry vulnerable. Every successful append closes that window.
		if err := f.syncDir(path.Dir(relativePath)); err != nil {
			return err
		}
		return nil
	})
}

// ReadJSONL returns each non-blank JSON object from relativePath in file
// order. The returned values are independent copies of the on-disk lines.
func (f *RootedFiles) ReadJSONL(relativePath string) ([]json.RawMessage, error) {
	if err := f.beginOperation(true); err != nil {
		return nil, err
	}
	defer f.endOperation()
	f.mu.RLock()
	defer f.mu.RUnlock()
	relativePath, err := f.scopedRelativePath(relativePath)
	if err != nil {
		return nil, err
	}
	file, err := f.openFile(relativePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open JSONL read %q: %w", relativePath, err)
	}
	defer func() { _ = f.fileOps.close(file) }()
	var records []json.RawMessage
	err = withRootedSharedFileLock(file, func() error {
		content, readErr := f.fileOps.readAll(file)
		if readErr != nil {
			return fmt.Errorf("dalgo2ingitdb: read JSONL %q: %w", relativePath, readErr)
		}
		if len(content) > 0 && content[len(content)-1] != '\n' {
			return fmt.Errorf("dalgo2ingitdb: interrupted JSONL tail at %q requires recovery before replay", relativePath)
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
	return f.writeJSONAtomic(relativePath, value, 0o644)
}

// WriteJSONAtomicWithMode serializes value and atomically publishes it at
// relativePath with the requested Unix permission bits. It is intended for
// callers whose records have a stricter storage classification than the
// default public projection mode used by WriteJSONAtomic.
func (f *RootedFiles) WriteJSONAtomicWithMode(relativePath string, value any, mode os.FileMode) error {
	if mode&^os.FileMode(0o777) != 0 {
		return fmt.Errorf("dalgo2ingitdb: JSON file mode %v contains non-permission bits", mode)
	}
	return f.writeJSONAtomic(relativePath, value, mode)
}

func (f *RootedFiles) writeJSONAtomic(relativePath string, value any, mode os.FileMode) (err error) {
	if err := f.beginOperation(true); err != nil {
		return err
	}
	defer f.endOperation()
	f.mu.RLock()
	defer f.mu.RUnlock()
	relativePath, err = f.scopedRelativePath(relativePath)
	if err != nil {
		return err
	}
	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: encode JSON %q: %w", relativePath, err)
	}
	content = append(content, '\n')
	f.creationMu.Lock()
	if err := f.mkdirParent(relativePath); err != nil {
		f.creationMu.Unlock()
		return err
	}
	// mkdirParent succeeded while mu is read-locked, so Close cannot clear
	// this descriptor before publication completes.
	root := f.root
	dir, base := path.Dir(relativePath), path.Base(relativePath)
	tempPath, err := rootedTemporaryPath(dir, base, f.fileOps.randomRead)
	if err != nil {
		f.creationMu.Unlock()
		return err
	}
	file, err := f.openFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	f.creationMu.Unlock()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: create JSON temporary file %q: %w", tempPath, err)
	}
	defer func() {
		if err != nil {
			_ = root.Remove(tempPath)
		}
	}()
	if _, err := f.fileOps.write(file, content); err != nil {
		_ = f.fileOps.close(file)
		return fmt.Errorf("dalgo2ingitdb: write JSON temporary file %q: %w", tempPath, err)
	}
	if err := f.fileOps.sync(file); err != nil {
		_ = f.fileOps.close(file)
		return fmt.Errorf("dalgo2ingitdb: sync JSON temporary file %q: %w", tempPath, err)
	}
	if err := f.fileOps.close(file); err != nil {
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
	if err := f.beginOperation(true); err != nil {
		return err
	}
	defer f.endOperation()
	f.mu.RLock()
	defer f.mu.RUnlock()
	relativePath, err := f.scopedRelativePath(relativePath)
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

// WithExclusiveLock runs fn while holding an advisory exclusive lock on a
// scoped relative file. Its operation lease remains live across fn, so Close
// waits for the callback while nested RootedFiles operations remain safe. The
// lock remains held until fn returns or ctx is cancelled while waiting to
// acquire it.
func (f *RootedFiles) WithExclusiveLock(ctx context.Context, relativePath string, fn func() error) error {
	if fn == nil {
		return errors.New("dalgo2ingitdb: rooted files lock callback is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A lock is always a top-level operation. In contrast, the ordinary file
	// operations below may re-enter from an already-running lock callback while
	// Close is draining it.
	if err := f.beginOperation(false); err != nil {
		return err
	}
	defer f.endOperation()
	f.mu.RLock()
	relativePath, err := f.scopedRelativePath(relativePath)
	if err != nil {
		f.mu.RUnlock()
		return err
	}
	f.creationMu.Lock()
	err = f.mkdirParent(relativePath)
	var file *os.File
	created := false
	if err == nil {
		root, rootErr := f.rootHandle()
		if rootErr != nil {
			err = rootErr
		} else {
			_, statErr := root.Stat(relativePath)
			created = errors.Is(statErr, fs.ErrNotExist)
			if statErr != nil && !created {
				err = fmt.Errorf("stat rooted lock %q: %w", relativePath, statErr)
			} else {
				file, err = f.openFile(relativePath, os.O_CREATE|os.O_RDWR, 0o600)
			}
		}
	}
	f.creationMu.Unlock()
	// Install this immediately after open: a directory sync below can fail and
	// must not leak the just-opened lock descriptor.
	if file != nil {
		defer func() { _ = f.fileOps.close(file) }()
	}
	if err == nil && created {
		err = f.syncDir(path.Dir(relativePath))
	}
	f.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: open rooted exclusive lock %q: %w", relativePath, err)
	}
	return withRootedExclusiveFileLockContext(ctx, file, func() error {
		f.enterCallback()
		defer f.exitCallback()
		return fn()
	})
}

// ReadDir returns the entries directly below a scoped relative directory.
func (f *RootedFiles) ReadDir(relativePath string) ([]os.DirEntry, error) {
	if err := f.beginOperation(true); err != nil {
		return nil, err
	}
	defer f.endOperation()
	f.mu.RLock()
	defer f.mu.RUnlock()
	relativePath, err := f.scopedRelativePath(relativePath)
	if err != nil {
		return nil, err
	}
	root, err := f.rootHandle()
	if err != nil {
		return nil, err
	}
	dir, err := root.Open(relativePath)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: open rooted directory %q: %w", relativePath, err)
	}
	entries, readErr := dir.ReadDir(-1)
	if closeErr := dir.Close(); readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: read rooted directory %q: %w", relativePath, readErr)
	}
	return entries, nil
}

func (f *RootedFiles) openFile(relativePath string, flag int, perm os.FileMode) (*os.File, error) {
	root, err := f.rootHandle()
	if err != nil {
		return nil, err
	}
	return root.OpenFile(relativePath, flag, perm)
}

func (f *RootedFiles) openJSONLAppendFile(relativePath string) (*os.File, bool, error) {
	// Different capabilities can race while establishing the first nested
	// directory chain. os.Root guarantees containment, but an open may observe
	// another writer between directory links; retry the bounded setup sequence
	// until the chain is observable. Once the file opens, its advisory lock is
	// the cross-capability publication boundary.
	for attempt := 0; attempt < 3; attempt++ {
		if err := f.mkdirParent(relativePath); err != nil {
			return nil, false, err
		}
		root := f.root
		_, statErr := root.Stat(relativePath)
		created := errors.Is(statErr, os.ErrNotExist)
		if statErr != nil && !created {
			return nil, false, fmt.Errorf("stat: %w", statErr)
		}
		file, err := f.openFile(relativePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
		if err == nil {
			return file, created, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, false, err
		}
	}
	return nil, false, errors.New("directory chain remained unavailable after concurrent rooted setup")
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
	current := ""
	for _, segment := range strings.Split(dir, "/") {
		parent := current
		if current == "" {
			current = segment
		} else {
			current = path.Join(current, segment)
		}
		if err := root.Mkdir(current, 0o755); err != nil {
			if errors.Is(err, fs.ErrExist) {
				continue
			}
			return fmt.Errorf("dalgo2ingitdb: create rooted directory %q: %w", current, err)
		}
		// A directory is durable only when its parent has recorded the new
		// entry. Sync both the parent and the child for every newly-created
		// link in the chain, rather than syncing just the final leaf.
		if parent == "" {
			parent = "."
		}
		if err := f.syncDir(parent); err != nil {
			return err
		}
		if err := f.syncDir(current); err != nil {
			return err
		}
	}
	return nil
}

func (f *RootedFiles) syncDir(relativePath string) error {
	root, err := f.rootHandle()
	if err != nil {
		return err
	}
	if f.syncDirectory == nil {
		return errors.New("dalgo2ingitdb: rooted directory sync is unavailable")
	}
	if err := f.syncDirectory(root, relativePath); err != nil {
		return fmt.Errorf("dalgo2ingitdb: sync JSON directory %q: %w", relativePath, err)
	}
	return nil
}

func syncRootDirectory(root *os.Root, relativePath string) error {
	dir, err := root.Open(relativePath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

// beginOperation obtains a lifecycle lease before taking mu. Close first marks
// the capability draining and then waits for all leases, so it cannot close the
// descriptor between a caller dropping mu and acquiring an advisory flock.
//
// A callback entered through WithExclusiveLock is already protected by an
// outer lease. Its ordinary nested file calls are admitted while Close drains;
// otherwise they could be blocked by RWMutex writer preference. New lock
// callbacks are never admitted once draining has begun.
func (f *RootedFiles) beginOperation(allowLockCallbackReentry bool) error {
	if f == nil {
		return errors.New("dalgo2ingitdb: rooted files is closed")
	}
	f.lifecycleMu.Lock()
	defer f.lifecycleMu.Unlock()
	if f.closed || (f.closing && (!allowLockCallbackReentry || f.callbacks == 0)) {
		return errors.New("dalgo2ingitdb: rooted files is closed")
	}
	f.operations++
	return nil
}

func (f *RootedFiles) endOperation() {
	f.lifecycleMu.Lock()
	f.operations--
	if f.operations == 0 {
		f.lifecycleConditionLocked().Broadcast()
	}
	f.lifecycleMu.Unlock()
}

func (f *RootedFiles) enterCallback() {
	f.lifecycleMu.Lock()
	f.callbacks++
	f.lifecycleMu.Unlock()
}

func (f *RootedFiles) exitCallback() {
	f.lifecycleMu.Lock()
	f.callbacks--
	if f.callbacks == 0 {
		f.lifecycleConditionLocked().Broadcast()
	}
	f.lifecycleMu.Unlock()
}

func (f *RootedFiles) lifecycleConditionLocked() *sync.Cond {
	if f.lifecycleCond == nil {
		f.lifecycleCond = sync.NewCond(&f.lifecycleMu)
	}
	return f.lifecycleCond
}

func (f *RootedFiles) rootHandle() (*os.Root, error) {
	if f == nil || f.root == nil {
		return nil, errors.New("dalgo2ingitdb: rooted files is closed")
	}
	return f.root, nil
}

func (f *RootedFiles) scopedRelativePath(relativePath string) (string, error) {
	return rootedRelativePath(relativePath)
}

// ensureRealRootedScope creates a configured scope if needed, but refuses a
// symlink at every scope component. Once OpenRoot below has opened that real
// directory, descendants cannot resolve through a symlink outside the scope.
func ensureRealRootedScope(root *os.Root, prefix string) (os.FileInfo, error) {
	info, err := root.Lstat(prefix)
	if errors.Is(err, fs.ErrNotExist) {
		err = root.Mkdir(prefix, 0o755)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("dalgo2ingitdb: create rooted files scope %q: %w", prefix, err)
		}
		if err == nil {
			if err := syncRootDirectory(root, "."); err != nil {
				return nil, fmt.Errorf("dalgo2ingitdb: sync rooted files scope parent: %w", err)
			}
			if err := syncRootDirectory(root, prefix); err != nil {
				return nil, fmt.Errorf("dalgo2ingitdb: sync rooted files scope %q: %w", prefix, err)
			}
		}
		info, err = root.Lstat(prefix)
	}
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: inspect rooted files scope %q: %w", prefix, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("dalgo2ingitdb: rooted files scope %q must be a real directory", prefix)
	}
	return info, nil
}

// recoverJSONLTail removes only an uncommitted final partial line. A JSONL
// record is committed only after its terminating newline has been synced. A
// malformed line that has a newline is already committed bytes and therefore
// fails rather than being silently rewritten.
func recoverJSONLTail(file *os.File, relativePath string, ops rootedFileOps) error {
	if _, err := ops.seek(file, 0, io.SeekStart); err != nil {
		return fmt.Errorf("dalgo2ingitdb: seek JSONL %q for recovery: %w", relativePath, err)
	}
	content, err := ops.readAll(file)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: read JSONL %q for recovery: %w", relativePath, err)
	}
	committed := content
	if len(content) > 0 && content[len(content)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(content, '\n')
		committed = content[:lastNewline+1]
	}
	for lineNumber, raw := range bytes.Split(committed, []byte{'\n'}) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) || line[0] != '{' {
			return fmt.Errorf("dalgo2ingitdb: invalid committed JSONL record at %q line %d", relativePath, lineNumber+1)
		}
	}
	if len(committed) != len(content) {
		if err := ops.truncate(file, int64(len(committed))); err != nil {
			return fmt.Errorf("dalgo2ingitdb: truncate interrupted JSONL tail at %q: %w", relativePath, err)
		}
		if err := ops.sync(file); err != nil {
			return fmt.Errorf("dalgo2ingitdb: sync recovered JSONL %q: %w", relativePath, err)
		}
	}
	if _, err := ops.seek(file, 0, io.SeekEnd); err != nil {
		return fmt.Errorf("dalgo2ingitdb: seek JSONL %q for append: %w", relativePath, err)
	}
	return nil
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

func rootedTemporaryPath(dir, base string, randomRead func([]byte) (int, error)) (string, error) {
	var random [12]byte
	if _, err := randomRead(random[:]); err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: generate temporary JSON file suffix: %w", err)
	}
	return path.Join(dir, "."+base+".tmp-"+hex.EncodeToString(random[:])), nil
}
