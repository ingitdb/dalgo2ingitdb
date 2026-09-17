package dalgo2ingitdb

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// Record keys substituted into a record_file.name template must stay a single
// path segment on every OS, otherwise an ID such as "a/b.txt" would be stored
// as a nested file that collection listings never see. Both separators are
// escaped regardless of the host OS so a repository reads the same everywhere.
// IDs without separators map to exactly the same file name as before.
var (
	recordKeyFileNameEscaper   = strings.NewReplacer("/", "%2F", `\`, "%5C")
	recordKeyFileNameUnescaper = strings.NewReplacer("%2F", "/", "%5C", `\`)
)

// recordKeyToFileName escapes path separators in a record key.
func recordKeyToFileName(recordKey string) string {
	return recordKeyFileNameEscaper.Replace(recordKey)
}

// recordKeyFromFileName reverses recordKeyToFileName.
func recordKeyFromFileName(name string) string {
	return recordKeyFileNameUnescaper.Replace(name)
}

// validateRecordPathSegment checks an ID that becomes one file or directory
// name (a record key substituted into record_file.name, or a parent record ID
// in a nested collection path). It rejects:
//   - "", "." and "..", which would name the containing or parent directory;
//   - control characters, which corrupt Git paths and YAML registries;
//   - the literal escape sequences %2F and %5C in any letter case, which would
//     share a file with
//     the ID holding the separator (e.g. "a%2Fb" and "a/b"). dalgo's
//     record.ValidateStringID reserves "%" for the same reason.
func validateRecordPathSegment(id string) error {
	switch id {
	case "", ".", "..":
		return fmt.Errorf("dalgo2ingitdb: record ID %q cannot be used as a file name", id)
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return fmt.Errorf("dalgo2ingitdb: record ID %q contains a control character", id)
		}
	}
	// Case-insensitive: on NTFS and default APFS "a%2fb" and "a%2Fb" name the
	// same file.
	if upper := strings.ToUpper(id); strings.Contains(upper, "%2F") || strings.Contains(upper, "%5C") {
		return fmt.Errorf("dalgo2ingitdb: record ID %q contains a reserved escape sequence (%%2F or %%5C)", id)
	}
	return nil
}

// validateRecordFileKey validates recordKey for a collection whose record file
// name is derived from the key, then verifies — as defense in depth — that the
// resolved record path stays inside the collection directory.
func validateRecordFileKey(colDef *ingitdb.CollectionDef, recordKey string) error {
	if !strings.Contains(colDef.RecordFile.Name, "{key}") {
		return nil
	}
	if err := validateRecordPathSegment(recordKey); err != nil {
		return err
	}
	return requireContainedPath(colDef.DirPath, resolveRecordPath(colDef, recordKey))
}

// requireContainedPath fails unless path is lexically strictly inside dir.
func requireContainedPath(dir, path string) error {
	rel, err := filepath.Rel(dir, path)
	if err != nil || rel == "." || !filepath.IsLocal(rel) {
		return fmt.Errorf("dalgo2ingitdb: resolved path %q escapes %q", path, dir)
	}
	return nil
}

// resolveRecordPath builds the on-disk path for a record by expanding the
// `{key}` placeholder in record_file.name and joining with the collection
// directory (plus the $records/ subdirectory when the name contains
// `{key}`). Path separators in the key are escaped so the record is always one
// file (see recordKeyToFileName).
func resolveRecordPath(colDef *ingitdb.CollectionDef, recordKey string) string {
	name := strings.ReplaceAll(colDef.RecordFile.Name, "{key}", recordKeyToFileName(recordKey))
	base := colDef.RecordFile.RecordsBasePath()
	return filepath.Join(colDef.DirPath, base, name)
}

// readSingleRecordFile reads one record file under a shared lock and
// returns the decoded record. Markdown and CSV formats are decoded via
// ingitdb.ParseRecordContentForCollection so column filtering and content_field
// handling apply.
//
// The bool reports whether the file exists, independent of read success:
//   - (nil, false, nil): the file does not exist.
//   - (nil, true, err):  the file exists but reading/parsing it failed.
//   - (data, true, nil): the file exists and decoded successfully.
//
// A stat failure other than "not exist" leaves existence unconfirmed and is
// returned as (nil, false, err).
func readSingleRecordFile(path string, colDef *ingitdb.CollectionDef) (map[string]any, bool, error) {
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat %s: %w", path, statErr)
	}
	var (
		data    map[string]any
		readErr error
	)
	if err := withSharedLock(path, func() error {
		content, err := osReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		data, readErr = ingitdb.ParseRecordContentForCollection(content, colDef)
		if readErr != nil {
			return fmt.Errorf("parse %s: %w", path, readErr)
		}
		return nil
	}); err != nil {
		// stat succeeded, so the file exists; only reading/parsing failed.
		return nil, true, err
	}
	return data, true, nil
}

// writeSingleRecordFile encodes data using the collection's format and
// writes it to path under an exclusive lock. Intermediate directories are
// created as needed.
func writeSingleRecordFile(path string, colDef *ingitdb.CollectionDef, data map[string]any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	content, err := ingitdb.EncodeRecordContentForCollection(data, colDef)
	if err != nil {
		return fmt.Errorf("encode record for %s: %w", path, err)
	}
	return withExclusiveLock(path, func() error {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		return nil
	})
}

// readMapOfRecordsFile reads a map-of-records file under a shared lock and
// returns all records keyed by ID. A missing file is not an error: it returns
// (nil, nil). Callers determine per-record existence with a key lookup on the
// returned map (a nil map yields "not present"), so no file-level "found" flag
// is needed — unlike single records, where the file is the record.
func readMapOfRecordsFile(path string, format ingitdb.RecordFormat) (map[string]map[string]any, error) {
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", path, statErr)
	}
	var result map[string]map[string]any
	if err := withSharedLock(path, func() error {
		content, err := osReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		result, err = ingitdb.ParseMapOfRecordsContent(content, format)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// writeMapOfRecordsFile encodes a full map-of-records dataset and writes
// it to path under an exclusive lock.
func writeMapOfRecordsFile(path string, colDef *ingitdb.CollectionDef, data map[string]map[string]any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	content, err := ingitdb.EncodeMapOfRecordsContent(data, colDef.RecordFile.Format, colDef.ID, colDef.ColumnsOrder)
	if err != nil {
		return fmt.Errorf("encode records for %s: %w", path, err)
	}
	return withExclusiveLock(path, func() error {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		return nil
	})
}

// deleteSingleRecordFile removes a record file under an exclusive lock.
// Deleting a record that does not exist is a no-op (returns nil), matching the
// idempotent Delete contract shared by dalgo adapters (e.g. dalgo2firestore).
//
// The pre-lock stat is required because gofrs/flock opens the lock target
// with O_CREATE; without this check we'd recreate the file just to delete it.
func deleteSingleRecordFile(path string) error {
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", path, statErr)
	}
	return withExclusiveLock(path, func() error {
		err := osRemove(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("remove %s: %w", path, err)
		}
		return nil
	})
}
