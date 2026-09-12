//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package dalgo2ingitdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func openRootedFiles(projectPath string, scope RootedFilesScope) (*RootedFiles, error) {
	return openRootedFilesWith(projectPath, scope, func(root *os.Root, name string) (*os.Root, error) {
		return root.OpenRoot(name)
	})
}

func withRootedSharedFileLock(file *os.File, fn func() error) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_SH); err != nil {
		return fmt.Errorf("dalgo2ingitdb: acquire rooted shared file lock: %w", err)
	}
	defer func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN) }()
	return fn()
}

func withRootedExclusiveFileLock(file *os.File, fn func() error) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("dalgo2ingitdb: acquire rooted exclusive file lock: %w", err)
	}
	defer func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN) }()
	return fn()
}

func withRootedExclusiveFileLockContext(ctx context.Context, file *os.File, fn func() error) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("dalgo2ingitdb: acquire rooted exclusive file lock: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN) }()
	return fn()
}
