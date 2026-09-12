//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
)

var errRootedFileLockUnsupported = errors.New("dalgo2ingitdb: rooted file locking is not supported on this platform")

func openRootedFiles(_ string, _ RootedFilesScope) (*RootedFiles, error) {
	return nil, errRootedFileLockUnsupported
}

func withRootedSharedFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}

func withRootedExclusiveFileLock(_ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}

func withRootedExclusiveFileLockContext(_ context.Context, _ *os.File, _ func() error) error {
	return errRootedFileLockUnsupported
}
