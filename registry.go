package dalgo2ingitdb

// specscore: feature/dalgo2ingitdb-dbschema-ddl-coverage
//
// registry.go maintains <projectPath>/.ingitdb/root-collections.yaml so
// the validator-backed CollectionsReader (used by loadDefinition for
// record transactions) stays in sync with on-disk collection
// directories. See REQ:auto-register-in-root-collections and
// REQ:auto-deregister-from-root-collections in the feature spec.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ingitdb/ingitdb-go/ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/config"
)

// writeRootCollectionsYAML writes root-collections.yaml with a real YAML
// encoder. config.WriteRootCollectionsToFile emits every name as a plain
// scalar, so a name such as "#notes" or one holding YAML indicators produced
// an unparseable registry that bricked the database. Names that are safe plain
// scalars serialize byte-identically to that writer (sorted "name: path" lines).
func writeRootCollectionsYAML(dirPath string, m map[string]string) error {
	if dirPath == "" {
		dirPath = "."
	}
	cfgDir := filepath.Join(dirPath, config.IngitDBDirName)
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		return fmt.Errorf("failed to create %s: %w", cfgDir, err)
	}
	var content []byte
	if len(m) > 0 {
		var err error
		if content, err = yamlMarshal(m); err != nil {
			return fmt.Errorf("failed to encode root collections: %w", err)
		}
	}
	if err := os.WriteFile(filepath.Join(cfgDir, config.RootCollectionsFileName), content, 0o644); err != nil {
		return fmt.Errorf("failed to write root collections file: %w", err)
	}
	return nil
}

// ErrCollectionPathConflict is returned by CreateCollection when
// <projectPath>/.ingitdb/root-collections.yaml already contains an
// entry for the collection name with a non-default path value. Callers
// either remove the existing entry or pick a different collection name.
var ErrCollectionPathConflict = errors.New("dalgo2ingitdb: root-collections.yaml entry conflicts with auto-registration")

// registerInRootCollections adds (or confirms) `name → name` in the
// project's root-collections.yaml registry. Idempotent per
// REQ:auto-register-in-root-collections:
//   - missing file → created with single entry
//   - file present, no entry → entry added, file rewritten sorted
//   - file present, entry equals (name, name) → no-op (byte-stable)
//   - file present, entry maps name → DIFFERENT path → wraps
//     ErrCollectionPathConflict and returns without writing
func registerInRootCollections(projectPath, name string) error {
	m, err := config.ReadRootCollectionsFromFile(projectPath, ingitdb.NewReadOptions())
	if err != nil {
		return fmt.Errorf("read root-collections.yaml: %w", err)
	}
	if existing, ok := m[name]; ok {
		if existing == name {
			// Idempotent path — byte-stable, no write.
			return nil
		}
		return fmt.Errorf(
			"%w: registry maps %q → %q but CreateCollection would register %q → %q",
			ErrCollectionPathConflict, name, existing, name, name,
		)
	}
	if m == nil {
		m = make(map[string]string, 1)
	}
	m[name] = name
	if err := writeRootCollections(projectPath, m); err != nil {
		return fmt.Errorf("write root-collections.yaml: %w", err)
	}
	return nil
}

// deregisterFromRootCollections removes `name` from the registry.
// Idempotent per REQ:auto-deregister-from-root-collections:
//   - missing file → no-op
//   - file present, no entry for name → no-op (but still re-writes the
//     file for consistency? no — to preserve byte stability, skip the
//     write when no change is needed)
//   - file present, entry exists → entry removed, file rewritten
//     (empty file if map becomes empty)
func deregisterFromRootCollections(projectPath, name string) error {
	m, err := config.ReadRootCollectionsFromFile(projectPath, ingitdb.NewReadOptions())
	if err != nil {
		return fmt.Errorf("read root-collections.yaml: %w", err)
	}
	if m == nil {
		// Registry never existed; nothing to deregister.
		return nil
	}
	if _, ok := m[name]; !ok {
		// Already absent — no change needed.
		return nil
	}
	delete(m, name)
	if err := writeRootCollections(projectPath, m); err != nil {
		return fmt.Errorf("write root-collections.yaml: %w", err)
	}
	return nil
}
