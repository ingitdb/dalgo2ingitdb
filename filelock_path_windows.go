//go:build windows

package dalgo2ingitdb

import (
	"fmt"
	"os"
)

// lockFilePath returns the file whose lock guards target. LockFileEx locks
// are mandatory and Go opens files without FILE_SHARE_DELETE, so locking
// target itself would make the lock holder's own write or remove of target
// fail. A per-target sidecar under the user cache directory is locked instead.
func lockFilePath(target string) (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve lock cache directory: %w", err)
	}
	return sidecarLockPath(cacheDir, target, true)
}
