//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package dalgo2ingitdb

import "os"

func rootedFileLockingSupported() bool { return false }

func withRootedSharedFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}

func withRootedExclusiveFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}
