//go:build !windows

package dalgo2ingitdb

// lockFilePath returns the file whose advisory lock guards target. flock(2)
// locks are advisory, so the target itself is locked: the holder can still
// write, replace and remove it, and processes running older driver versions
// keep excluding each other on the same file.
func lockFilePath(target string) (string, error) {
	return target, nil
}
