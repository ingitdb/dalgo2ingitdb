package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

func listCountryKeys(t *testing.T, db dal.DB) []string {
	t.Helper()
	q := dal.From(dal.NewRootCollectionRef("countries", "")).NewQuery().
		SelectIntoRecord(func() record.Record {
			return record.NewRecordWithData(record.NewKeyWithID("countries", ""), map[string]any{})
		})
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var keys []string
	for {
		rec, nextErr := reader.Next()
		if errors.Is(nextErr, dal.ErrNoMoreRecords) {
			break
		}
		if nextErr != nil {
			t.Fatalf("reader.Next: %v", nextErr)
		}
		keys = append(keys, rec.Key().ID.(string))
	}
	sort.Strings(keys)
	return keys
}

// Record IDs containing path separators must map to one file directly under
// $records/ so collection listings return them (ingitdb/dalgo2ingitdb#13).
func TestDB_RecordIDsWithPathSeparators(t *testing.T) {
	t.Parallel()
	db, root := setupSingleRecordDB(t)
	ctx := context.Background()
	records := filepath.Join(root, "countries", "$records")

	ids := map[string]string{
		"a/b.txt":  "a%2Fb.txt.yaml",
		`c\d`:      "c%5Cd.yaml",
		"plain":    "plain.yaml",
		"50%off":   "50%off.yaml",
		"nested/x": "nested%2Fx.yaml",
	}
	for id := range ids {
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", id), map[string]any{"name": id}))
		})
		if err != nil {
			t.Fatalf("Set %q: %v", id, err)
		}
	}

	entries, err := os.ReadDir(records)
	if err != nil {
		t.Fatalf("read records dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			t.Errorf("unexpected directory under $records: %s", e.Name())
		}
		names = append(names, e.Name())
	}
	var wantNames []string
	var wantKeys []string
	for id, name := range ids {
		wantNames = append(wantNames, name)
		wantKeys = append(wantKeys, id)
	}
	sort.Strings(names)
	sort.Strings(wantNames)
	sort.Strings(wantKeys)
	if len(names) != len(wantNames) {
		t.Fatalf("files: got %v, want %v", names, wantNames)
	}
	for i := range names {
		if names[i] != wantNames[i] {
			t.Errorf("files[%d]: got %q, want %q", i, names[i], wantNames[i])
		}
	}

	keys := listCountryKeys(t, db)
	if len(keys) != len(wantKeys) {
		t.Fatalf("listed keys: got %q, want %q", keys, wantKeys)
	}
	for i := range keys {
		if keys[i] != wantKeys[i] {
			t.Errorf("keys[%d]: got %q, want %q", i, keys[i], wantKeys[i])
		}
	}

	got := record.NewRecordWithData(record.NewKeyWithID("countries", "a/b.txt"), map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Fatalf("Get a/b.txt: %v", err)
	}
	if name := got.Data().(map[string]any)["name"]; name != "a/b.txt" {
		t.Errorf("Get a/b.txt: name = %v", name)
	}

	if err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Delete(ctx, record.NewKeyWithID("countries", "a/b.txt"))
	}); err != nil {
		t.Fatalf("Delete a/b.txt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(records, "a%2Fb.txt.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("after Delete: stat err = %v, want not exist", err)
	}
}

// IDs that literally contain an escape sequence would collide with an escaped
// separator, so writes and reads reject them instead of aliasing another record.
func TestDB_RecordIDsContainingEscapeSequencesRejected(t *testing.T) {
	t.Parallel()
	db, _ := setupSingleRecordDB(t)
	ctx := context.Background()
	for _, id := range []string{"a%2Fb", `a%5Cb`, "a%2fb", "a%5cb"} {
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", id), map[string]any{"name": id}))
		})
		if err == nil {
			t.Errorf("Set %q: want error, got nil", id)
		}
		got := record.NewRecordWithData(record.NewKeyWithID("countries", id), map[string]any{})
		if err := db.Get(ctx, got); err == nil || errors.Is(err, record.ErrRecordNotFound) {
			t.Errorf("Get %q: want invalid-id error, got %v", id, err)
		}
	}
}

// On case-insensitive file systems "a%2fb.yaml" and "a%2Fb.yaml" are one file,
// so after storing "a/b" the lower-case spelling must not alias it.
func TestDB_RecordIDEscapeCheckIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	db, root := setupSingleRecordDB(t)
	ctx := context.Background()
	set := func(id, name string) error {
		return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", id), map[string]any{"name": name}))
		})
	}
	if err := set("a/b", "original"); err != nil {
		t.Fatalf("Set a/b: %v", err)
	}
	if err := set("a%2fb", "clobber"); err == nil {
		t.Fatal(`Set "a%2fb": want error, got nil`)
	}
	data, err := os.ReadFile(filepath.Join(root, "countries", "$records", "a%2Fb.yaml"))
	if err != nil || string(data) != "name: original\n" {
		t.Errorf("a/b record changed: %q, %v", data, err)
	}
}
