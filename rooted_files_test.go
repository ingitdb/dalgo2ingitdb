package dalgo2ingitdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRootedFilesAppendAndReadJSONL(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files, err := OpenRootedFiles(root)
	if err != nil {
		t.Fatalf("OpenRootedFiles: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })

	const path = "incidents/INC-1/events.jsonl"
	if err := files.AppendJSONL(path, map[string]any{"seq": 1, "type": "created"}); err != nil {
		t.Fatalf("AppendJSONL first: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read first event file: %v", err)
	}
	if err := files.AppendJSONL(path, map[string]any{"seq": 2, "type": "note"}); err != nil {
		t.Fatalf("AppendJSONL second: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read appended event file: %v", err)
	}
	if string(after[:len(before)]) != string(before) {
		t.Fatalf("AppendJSONL rewrote existing bytes: before=%q after=%q", before, after)
	}

	records, err := files.ReadJSONL(path)
	if err != nil {
		t.Fatalf("ReadJSONL: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("ReadJSONL record count = %d, want 2", len(records))
	}
	for i, want := range []int{1, 2} {
		var record struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(records[i], &record); err != nil {
			t.Fatalf("decode record %d: %v", i, err)
		}
		if record.Seq != want {
			t.Errorf("record %d seq = %d, want %d", i, record.Seq, want)
		}
	}
}

func TestRootedFilesWriteAndReadJSON(t *testing.T) {
	t.Parallel()
	files, err := OpenRootedFiles(t.TempDir())
	if err != nil {
		t.Fatalf("OpenRootedFiles: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })

	const path = "incidents/INC-1/incident.json"
	want := struct {
		Title string `json:"title"`
		Seq   int    `json:"seq"`
	}{Title: "database unavailable", Seq: 2}
	if err := files.WriteJSONAtomic(path, want); err != nil {
		t.Fatalf("WriteJSONAtomic: %v", err)
	}
	var got struct {
		Title string `json:"title"`
		Seq   int    `json:"seq"`
	}
	if err := files.ReadJSON(path, &got); err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	if got != want {
		t.Errorf("ReadJSON = %+v, want %+v", got, want)
	}
}

func TestRootedFilesRejectsEscapingPaths(t *testing.T) {
	t.Parallel()
	files, err := OpenRootedFiles(t.TempDir())
	if err != nil {
		t.Fatalf("OpenRootedFiles: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })

	for _, path := range []string{"../outside.jsonl", "/outside.jsonl", "incidents/../../outside.jsonl"} {
		if err := files.AppendJSONL(path, map[string]any{"seq": 1}); err == nil {
			t.Errorf("AppendJSONL(%q): want escape refusal", path)
		}
	}
	if err := files.AppendJSONL("events.jsonl", []int{1}); err == nil {
		t.Error("AppendJSONL non-object: want error")
	}
}

func TestRootedFilesSerializesConcurrentAppends(t *testing.T) {
	files, err := OpenRootedFiles(t.TempDir())
	if err != nil {
		t.Fatalf("OpenRootedFiles: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })

	const count = 32
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			if err := files.AppendJSONL("incidents/INC-1/events.jsonl", map[string]any{"seq": i}); err != nil {
				t.Errorf("AppendJSONL(%d): %v", i, err)
			}
		})
	}
	wg.Wait()
	records, err := files.ReadJSONL("incidents/INC-1/events.jsonl")
	if err != nil {
		t.Fatalf("ReadJSONL: %v", err)
	}
	if len(records) != count {
		t.Fatalf("ReadJSONL record count = %d, want %d", len(records), count)
	}
	seen := make(map[int]bool, count)
	for _, raw := range records {
		var record struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatalf("decode record: %v", err)
		}
		seen[record.Seq] = true
	}
	for i := range count {
		if !seen[i] {
			t.Errorf("missing concurrent JSONL record %d", i)
		}
	}
}

func TestRootedFilesKeepsOpenedRootAcrossPathSwap(t *testing.T) {
	rootParent := t.TempDir()
	root := filepath.Join(rootParent, "repository")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}
	outside := filepath.Join(rootParent, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatalf("create outside: %v", err)
	}
	files, err := OpenRootedFiles(root)
	if err != nil {
		t.Fatalf("OpenRootedFiles: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })

	moved := filepath.Join(rootParent, "repository-moved")
	if err := os.Rename(root, moved); err != nil {
		t.Fatalf("move root: %v", err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if err := files.AppendJSONL("incidents/INC-1/events.jsonl", map[string]any{"seq": 1}); err != nil {
		t.Fatalf("AppendJSONL after root swap: %v", err)
	}
	if err := files.WriteJSONAtomic("incidents/INC-1/incident.json", map[string]any{"seq": 1}); err != nil {
		t.Fatalf("WriteJSONAtomic after root swap: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, "incidents", "INC-1", "events.jsonl")); err != nil {
		t.Fatalf("opened root did not receive event: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, "incidents", "INC-1", "incident.json")); err != nil {
		t.Fatalf("opened root did not receive projection: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "incidents")); !os.IsNotExist(err) {
		t.Fatalf("swapped symlink target changed: stat error = %v, want not exist", err)
	}
}

func TestRootedFilesForDALgoDB(t *testing.T) {
	t.Parallel()
	db, err := NewDatabase(t.TempDir(), newReader())
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	files, err := RootedFilesFor(db)
	if err != nil {
		t.Fatalf("RootedFilesFor: %v", err)
	}
	t.Cleanup(func() { _ = files.Close() })
	if err := files.AppendJSONL("events.jsonl", map[string]any{"seq": 1}); err != nil {
		t.Fatalf("AppendJSONL: %v", err)
	}

	if _, err := RootedFilesFor(nil); err == nil {
		t.Fatal("RootedFilesFor(nil): want error")
	}
}
