package dalgo2ingitdb_test

// specscore: feature/record-key-encoding (ingitdb-go)

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

const tildeDefinition = `record_file:
    name: '{key}.yaml'
    format: yaml
    type: map[string]any
    key_encoding: tilde-v1
columns:
    name:
        type: string
columns_order:
    - name
`

// tildeDB is spaces > notes, both declaring key_encoding tilde-v1, loaded
// from definition files (so the setting also passes strict decoding).
func tildeDB(t *testing.T) (dal.DB, string) {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		".ingitdb/root-collections.yaml":                          "spaces: spaces\n",
		"spaces/.collection/definition.yaml":                      tildeDefinition,
		"spaces/.collection/subcollections/notes/definition.yaml": tildeDefinition,
	} {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	return db, root
}

func set(ctx context.Context, db dal.DB, key *record.Key, name string) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(key, map[string]any{"name": name}))
	})
}

func names(t *testing.T, db dal.DB, ref dal.CollectionRef) ([]string, error) {
	t.Helper()
	q := dal.NewQueryBuilder(dal.From(ref)).SelectIntoRecord(func() record.Record {
		return record.NewRecordWithData(record.NewKeyWithID(ref.Name(), ""), map[string]any{})
	})
	var ids []string
	err := db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		for {
			rec, nextErr := reader.Next()
			if nextErr != nil {
				return nil
			}
			ids = append(ids, rec.Key().ID.(string))
		}
	})
	return ids, err
}

// Verifies record-key-encoding#ac:readers-use-decoded-keys for dalgo2ingitdb:
// keys that the escaping of collections without an encoding refuses ("..",
// "a%2Fb", control bytes) are stored under their tilde-v1 names and read back
// as the real ids.
func TestKeyEncoding_TildeV1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, root := tildeDB(t)
	space := record.NewKeyWithID("spaces", "a/b")
	note := record.NewKeyWithParentAndID(space, "notes", "..")
	for key, name := range map[*record.Key]string{space: "s", record.NewKeyWithID("spaces", "x%2Fy"): "p", note: "n"} {
		if err := set(ctx, db, key, name); err != nil {
			t.Fatalf("Set %v: %v", key, err)
		}
	}
	for _, p := range []string{"spaces/$records/a~2Fb.yaml", "spaces/$records/x~252Fy.yaml", "spaces/$records/a~2Fb/notes/$records/~2E~2E.yaml"} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err != nil {
			t.Errorf("expected %s: %v", p, err)
		}
	}
	data := map[string]any{}
	if err := db.Get(ctx, record.NewRecordWithData(note, data)); err != nil || data["name"] != "n" {
		t.Errorf("Get note: %v, %v", data, err)
	}
	if ids, err := names(t, db, dal.NewRootCollectionRef("spaces", "")); err != nil || strings.Join(ids, " ") != "a/b x%2Fy" {
		t.Errorf("query spaces: %q, %v", ids, err)
	}
	if ids, err := names(t, db, dal.NewCollectionRef("notes", "", space)); err != nil || strings.Join(ids, " ") != ".." {
		t.Errorf("query notes: %q, %v", ids, err)
	}
}

// Verifies record-key-encoding#ac:invalid-names-and-collisions-reported and
// #ac:over-long-key-fails for dalgo2ingitdb.
func TestKeyEncoding_TildeV1Refusals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, root := tildeDB(t)
	if err := set(ctx, db, record.NewKeyWithID("spaces", "ab"), "ab"); err != nil {
		t.Fatal(err)
	}
	if err := set(ctx, db, record.NewKeyWithID("spaces", "AB"), "x"); !errors.Is(err, dalgo2ingitdb.ErrNameDiffersInCase) || !strings.Contains(err.Error(), "ab.yaml exists;") {
		t.Errorf("Set case-only difference: %v", err)
	}
	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record.NewRecordWithData(record.NewKeyWithID("spaces", "Ab"), map[string]any{"name": "x"}))
	}); !errors.Is(err, dalgo2ingitdb.ErrNameDiffersInCase) {
		t.Errorf("Insert case-only difference: %v", err)
	}
	long := strings.Repeat("é", 43)
	if err := set(ctx, db, record.NewKeyWithID("spaces", long), "x"); !errors.Is(err, ingitdb.ErrKeyNotStorable) {
		t.Errorf("over-long key: %v", err)
	}
	if err := set(ctx, db, record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", strings.Repeat("/", 90)), "notes", "n"), "x"); !errors.Is(err, ingitdb.ErrKeyNotStorable) {
		t.Errorf("over-long parent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "spaces", "$records", ".#ab.yaml"), []byte("name: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ids, err := names(t, db, dal.NewRootCollectionRef("spaces", "")); err != nil || strings.Join(ids, " ") != "ab" {
		t.Errorf("a hidden file is skipped: %q, %v", ids, err)
	}
	// A parent directory that differs only in case is refused too.
	if err := set(ctx, db, record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "Pq"), "notes", "n"), "x"); err != nil {
		t.Fatal(err)
	}
	if err := set(ctx, db, record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "pq"), "notes", "m"), "x"); !errors.Is(err, dalgo2ingitdb.ErrNameDiffersInCase) || !strings.Contains(err.Error(), filepath.Join("$records", "Pq")+" exists") {
		t.Errorf("Set under a parent directory differing in case: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "spaces", "$records", "a b.yaml"), []byte("name: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := names(t, db, dal.NewRootCollectionRef("spaces", "")); !errors.Is(err, ingitdb.ErrInvalidKeyName) {
		t.Errorf("query of a name the encoding never writes: %v", err)
	}
}

const plainDefinition = `record_file:
    name: '{key}.yaml'
    format: yaml
    type: map[string]any
columns:
    name:
        type: string
columns_order:
    - name
`

// Every one-record-per-file collection fails an over-long key with
// ErrKeyNotStorable and the remedy, measured on the name actually written
// (path separators escaped), for the record file and a parent directory.
func TestLengthRule_NoKeyEncoding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	for name, content := range map[string]string{
		".ingitdb/root-collections.yaml":                          "spaces: spaces\n",
		"spaces/.collection/definition.yaml":                      plainDefinition,
		"spaces/.collection/subcollections/notes/definition.yaml": plainDefinition,
	} {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	if err := set(ctx, db, record.NewKeyWithID("spaces", strings.Repeat("k", 250)), "fits"); err != nil {
		t.Errorf("a 255-byte name: %v", err)
	}
	if err := set(ctx, db, record.NewKeyWithID("spaces", strings.Repeat("k", 251)), "x"); !errors.Is(err, ingitdb.ErrKeyNotStorable) || !strings.Contains(err.Error(), "record_file.type") {
		t.Errorf("a 256-byte name: %v", err)
	}
	if err := set(ctx, db, record.NewKeyWithID("spaces", strings.Repeat("a/", 70)+"a"), "x"); !errors.Is(err, ingitdb.ErrKeyNotStorable) {
		t.Errorf("a key that escapes to an over-long name: %v", err)
	}
	if err := set(ctx, db, record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", strings.Repeat("p", 256)), "notes", "n"), "x"); !errors.Is(err, ingitdb.ErrKeyNotStorable) {
		t.Errorf("an over-long parent directory: %v", err)
	}
}
