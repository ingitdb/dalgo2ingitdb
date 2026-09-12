//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package dalgo2ingitdb

import (
	"errors"
	"os"
)

var errRootedFileLockUnsupported = errors.New("dalgo2ingitdb: rooted file locking is not supported on this platform")

func withRootedSharedFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}

func withRootedExclusiveFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}
