//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"testing"
)

var incidentScope = RootedFilesScope{Prefix: "incidents"}

func TestRootedFilesUnsupportedPlatformFailsBeforeFilesystemMutation(t *testing.T) {
	root := t.TempDir()
	db, err := NewDatabase(root, newReader(), WithRootedFilesScopes(incidentScope))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RootedFilesFor(context.Background(), db, incidentScope); !errors.Is(err, errRootedFileLockUnsupported) {
		t.Fatalf("RootedFilesFor error = %v, want unsupported file locking", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported capability mutated root: %v", entries)
	}
}
