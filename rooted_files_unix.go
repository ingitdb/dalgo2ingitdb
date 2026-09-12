//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package dalgo2ingitdb

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func rootedFileLockingSupported() bool { return true }

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
