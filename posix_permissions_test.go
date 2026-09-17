package dalgo2ingitdb

import (
	"runtime"
	"testing"
)

// skipWithoutPOSIXPermissions skips tests that provoke I/O errors by removing
// POSIX permission bits (os.Chmod); Windows ignores those bits.
func skipWithoutPOSIXPermissions(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}
}
