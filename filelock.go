package dalgo2ingitdb

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofrs/flock"
)

// fileLocker is the advisory-lock surface used by withSharedLock and
// withExclusiveLock. *flock.Flock satisfies it; tests inject a mock via the
// newFileLocker seam to exercise lock-acquisition failures.
type fileLocker interface {
	Lock() error
	RLock() error
	Unlock() error
}

// withSharedLock acquires a shared (read) advisory lock on the file at
// path, calls fn, then releases the lock. On Unix the lock is provided by
// syscall.Flock on the target file itself, so no sidecar files are created.
// On Windows LockFileEx locks are mandatory — a lock on the target would
// refuse the holder's own writes through a second handle and its deletes — so
// the lock is taken on a sidecar file outside the project instead (see
// lockFilePath).
//
// Multiple goroutines / processes may hold simultaneous shared locks on
// the same file. An attempt to acquire an exclusive lock while any shared
// lock is held blocks until all shared locks release.
//
// The lock is released even when fn returns an error.
func withSharedLock(path string, fn func() error) error {
	lk := newFileLocker(path)
	if err := lk.RLock(); err != nil {
		return fmt.Errorf("acquire shared lock on %s: %w", path, err)
	}
	defer func() {
		_ = lk.Unlock()
	}()
	return fn()
}

// withExclusiveLock acquires an exclusive (write) advisory lock on the
// file at path, calls fn, then releases the lock. Only one holder may
// have an exclusive lock at a time; the call blocks while any other
// shared or exclusive lock is held.
//
// The lock is released even when fn returns an error.
func withExclusiveLock(path string, fn func() error) error {
	lk := newFileLocker(path)
	if err := lk.Lock(); err != nil {
		return fmt.Errorf("acquire exclusive lock on %s: %w", path, err)
	}
	defer func() {
		_ = lk.Unlock()
	}()
	return fn()
}

// defaultFileLocker is the production newFileLocker: a gofrs/flock lock on the
// platform lock path for target. A failure to prepare the lock path surfaces
// as a lock-acquisition error.
func defaultFileLocker(target string) fileLocker {
	lockPath, err := lockFilePath(target)
	if err != nil {
		return failedFileLocker{err: err}
	}
	return flock.New(lockPath)
}

// failedFileLocker reports a lock-path preparation error from Lock and RLock.
type failedFileLocker struct{ err error }

func (l failedFileLocker) Lock() error   { return l.err }
func (l failedFileLocker) RLock() error  { return l.err }
func (l failedFileLocker) Unlock() error { return nil }

// sidecarLockPath maps target to a stable lock file under
// <cacheDir>/dalgo2ingitdb/file-locks, named by the SHA-256 of the absolute,
// cleaned target path (lower-cased when foldCase, for case-insensitive file
// systems). Every cooperating process resolving the same target therefore
// locks the same sidecar, and nothing is created inside the project tree.
func sidecarLockPath(cacheDir, target string, foldCase bool) (string, error) {
	abs, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve lock target %s: %w", target, err)
	}
	if foldCase {
		abs = strings.ToLower(abs)
	}
	dir := filepath.Join(cacheDir, "dalgo2ingitdb", "file-locks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create lock directory %s: %w", dir, err)
	}
	hash := sha256.Sum256([]byte(abs))
	return filepath.Join(dir, fmt.Sprintf("%x.lock", hash[:])), nil
}
