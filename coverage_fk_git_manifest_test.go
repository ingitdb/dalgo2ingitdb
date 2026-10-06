package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

func TestForeignKeys_Gaps(t *testing.T) {
	// line 17: validateWriteForeignKeys with r.def == nil
	rEmpty := readwriteTx{}
	col := &ingitdb.CollectionDef{ID: "c", Columns: map[string]*ingitdb.ColumnDef{"fk": {Type: ingitdb.ColumnTypeString, ForeignKey: "p"}}}
	err := rEmpty.validateWriteForeignKeys("Insert", "c", col, map[string]any{"fk": "1"})
	if err == nil {
		t.Fatal("validateWriteForeignKeys: want error when def is nil")
	}

	// line 23: parentCollection == "" continue
	colEmptyFK := &ingitdb.CollectionDef{ID: "c", Columns: map[string]*ingitdb.ColumnDef{"col": {Type: ingitdb.ColumnTypeString}}}
	def := &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"c": colEmptyFK}}
	rWithDef := readwriteTx{readonlyTx: readonlyTx{def: def}}
	if err := rWithDef.validateWriteForeignKeys("Insert", "c", colEmptyFK, map[string]any{"col": "val"}); err != nil {
		t.Fatalf("validateWriteForeignKeys with empty FK: %v", err)
	}

	// line 44: recordExists error (e.g. parent key with control char)
	colWithFK := &ingitdb.CollectionDef{
		ID:      "c",
		Columns: map[string]*ingitdb.ColumnDef{"fk": {Type: ingitdb.ColumnTypeString, ForeignKey: "p"}},
	}
	parentDef := &ingitdb.CollectionDef{
		ID: "p",
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"id": {Type: ingitdb.ColumnTypeString}},
	}
	def.Collections["c"] = colWithFK
	def.Collections["p"] = parentDef
	// line 41: recordExists error (parent collection RecordFile is nil)
	parentDef.RecordFile = nil
	err = rWithDef.validateWriteForeignKeys("Insert", "c", colWithFK, map[string]any{"fk": "val"})
	if err == nil {
		t.Fatal("validateWriteForeignKeys: want error when parent RecordFile is nil")
	}
	parentDef.RecordFile = &ingitdb.RecordFileDef{
		RecordType: ingitdb.SingleRecord,
		Format:     ingitdb.RecordFormatYAML,
		Name:       "{key}.yaml",
	}

	// line 107: validateDeleteForeignKeys with r.def == nil
	if err := rEmpty.validateDeleteForeignKeys("p", "k1"); err == nil {
		t.Fatal("validateDeleteForeignKeys: want error when def is nil")
	}

	// Remove c from def.Collections so its missing RecordFile does not panic in delete scan
	delete(def.Collections, "c")

	// line 120: readAllSingleRecords error in validateDeleteForeignKeys
	brokenChild := &ingitdb.CollectionDef{
		ID:      "broken_child",
		DirPath: t.TempDir(),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"fk": {Type: ingitdb.ColumnTypeString, ForeignKey: "p"}},
	}
	def.Collections["broken_child"] = brokenChild

	origReadAll := readAllRecords
	readAllRecords = func(colDef *ingitdb.CollectionDef) ([]record.Record, error) {
		return nil, errors.New("read failed")
	}
	err = rWithDef.validateDeleteForeignKeys("p", "k1")
	if err == nil {
		t.Fatal("validateDeleteForeignKeys: want error when readAllRecords fails")
	}

	// line 134-135: child collection record data is not map[string]any
	readAllRecords = func(colDef *ingitdb.CollectionDef) ([]record.Record, error) {
		k := record.NewKeyWithID(colDef.ID, "rec1")
		return []record.Record{record.NewRecordWithData(k, "not-a-map")}, nil
	}
	err = rWithDef.validateDeleteForeignKeys("p", "k1")
	readAllRecords = origReadAll
	if err == nil || !strings.Contains(err.Error(), "data has type") {
		t.Fatalf("validateDeleteForeignKeys: want data has type error, got %v", err)
	}

	// line 124-127: child collection records sorting
	childRoot := t.TempDir()
	childCol := &ingitdb.CollectionDef{
		ID:      "child_sort",
		DirPath: childRoot,
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"fk": {Type: ingitdb.ColumnTypeString, ForeignKey: "p"}},
	}
	def.Collections = map[string]*ingitdb.CollectionDef{"p": parentDef, "child_sort": childCol}
	recDir := filepath.Join(childRoot, childCol.RecordFile.RecordsBasePath())
	_ = os.MkdirAll(recDir, 0o755)
	_ = os.WriteFile(filepath.Join(recDir, "b.yaml"), []byte("fk: other\n"), 0o644)
	_ = os.WriteFile(filepath.Join(recDir, "a.yaml"), []byte("fk: other\n"), 0o644)
	if err := rWithDef.validateDeleteForeignKeys("p", "k1"); err != nil {
		t.Fatalf("validateDeleteForeignKeys sorting: %v", err)
	}

	// line 216: orderedCollectionIDs(nil)
	if res := orderedCollectionIDs(nil); res != nil {
		t.Fatalf("orderedCollectionIDs(nil) = %v, want nil", res)
	}

	// line 256: isEmptyForeignKeyValue(nil)
	if !isEmptyForeignKeyValue(nil) {
		t.Fatal("isEmptyForeignKeyValue(nil) want true")
	}

	// line 260: isEmptyForeignKeyValue(123)
	if isEmptyForeignKeyValue(123) {
		t.Fatal("isEmptyForeignKeyValue(123) want false")
	}

	// line 267: foreignKeyTargetExists(nil, ...)
	if _, err := foreignKeyTargetExists(nil, "k"); err == nil {
		t.Fatal("foreignKeyTargetExists(nil): want error")
	}

	// line 270: foreignKeyTargetExists(&ingitdb.CollectionDef{RecordFile: nil}, ...)
	if _, err := foreignKeyTargetExists(&ingitdb.CollectionDef{ID: "c", RecordFile: nil}, "k"); err == nil {
		t.Fatal("foreignKeyTargetExists with nil RecordFile: want error")
	}
}

func TestGitCommit_Gaps(t *testing.T) {
	// line 28-30: restoreSnapshots with non-empty directory for deleted path
	tempDir := t.TempDir()
	nonEmptyDir := filepath.Join(tempDir, "non_empty_dir")
	_ = os.MkdirAll(nonEmptyDir, 0o755)
	_ = os.WriteFile(filepath.Join(nonEmptyDir, "file.txt"), []byte("content"), 0o644)
	snaps := map[string]fileSnapshot{
		nonEmptyDir: {exists: false},
	}
	if err := restoreSnapshots(snaps); err == nil {
		t.Fatal("restoreSnapshots: want error on non-empty dir remove")
	}

	// line 33-35: restoreSnapshots with file blocking directory creation
	fileBlockingDir := filepath.Join(tempDir, "file_blocking_dir")
	_ = os.WriteFile(fileBlockingDir, []byte("x"), 0o644)
	childPath := filepath.Join(fileBlockingDir, "child", "file.txt")
	snaps2 := map[string]fileSnapshot{
		childPath: {exists: true, data: []byte("abc"), mode: 0o644},
	}
	if err := restoreSnapshots(snaps2); err == nil {
		t.Fatal("restoreSnapshots: want error on mkdir failure")
	}

	// line 36-38: restoreSnapshots with directory blocking file write
	dirBlockingFile := filepath.Join(tempDir, "dir_blocking_file")
	_ = os.MkdirAll(dirBlockingFile, 0o755)
	snaps3 := map[string]fileSnapshot{
		dirBlockingFile: {exists: true, data: []byte("abc"), mode: 0o644},
	}
	if err := restoreSnapshots(snaps3); err == nil {
		t.Fatal("restoreSnapshots: want error on write failure")
	}

	// line 51-53: gitCommitPaths with len(staged) == 0
	if err := gitCommitPaths(context.Background(), tempDir, nil, "msg"); err != nil {
		t.Fatalf("gitCommitPaths empty: %v", err)
	}

	// line 71-74: osCreateTemp error in gitCommitPaths
	origCreateTemp := osCreateTemp
	osCreateTemp = func(string, string) (*os.File, error) {
		return nil, errors.New("temp error")
	}
	// Need repoDir to look like a git work tree
	_ = exec.Command("git", "init", tempDir).Run()
	err := gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "file.txt")}, "msg")
	osCreateTemp = origCreateTemp
	if err == nil {
		t.Fatal("gitCommitPaths: want error when osCreateTemp fails")
	}

	// line 139-141: gitHead with canceled context
	cancCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = gitHead(cancCtx, tempDir)
	if err == nil {
		t.Fatal("gitHead: want error with canceled context")
	}

	// line 146: gitHead error on invalid repo dir
	_, _, err = gitHead(context.Background(), filepath.Join(tempDir, "nonexistent"))
	if err == nil {
		t.Fatal("gitHead: want error on nonexistent repo")
	}

	// Seam gitHeadSeam error (line 86)
	origGitHead := gitHeadSeam
	defer func() { gitHeadSeam = origGitHead }()
	gitHeadSeam = func(ctx context.Context, repoDir string) (string, bool, error) {
		return "", false, errors.New("head failure")
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "head failure") {
		t.Fatalf("gitCommitPaths: want head failure error, got %v", err)
	}
	gitHeadSeam = origGitHead

	// Seam gitCmdRun to test gitCommitPaths plumbing error branches:
	origGitCmdRun := gitCmdRun
	defer func() { gitCmdRun = origGitCmdRun }()

	// line 94-96 / 97-99: read-tree error without HEAD
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "read-tree" {
			return []byte("read-tree error"), errors.New("read-tree failure")
		}
		return []byte(""), nil
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "temporary index") {
		t.Fatalf("gitCommitPaths: want read-tree error, got %v", err)
	}

	// Now create initial commit so hasHead is true:
	gitCmdRun = origGitCmdRun
	if err := os.WriteFile(filepath.Join(tempDir, "f.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", tempDir, "add", ".").CombinedOutput(); err != nil {
		t.Fatalf("seed git add: %v: %s", err, out)
	}
	if out, err := exec.Command("git", "-C", tempDir, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "init").CombinedOutput(); err != nil {
		t.Fatalf("seed git commit: %v: %s", err, out)
	}

	// line 90: read-tree error with HEAD
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "read-tree" {
			return []byte("read-tree error"), errors.New("read-tree failure")
		}
		return []byte(""), nil
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "seed temporary index") {
		t.Fatalf("gitCommitPaths: want seed temporary index error, got %v", err)
	}

	// line 101-103: git add error
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "add" {
			return []byte("add error"), errors.New("add failure")
		}
		return []byte(""), nil
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "git add") {
		t.Fatalf("gitCommitPaths: want git add error, got %v", err)
	}

	// line 105-107: write-tree error
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "write-tree" {
			return []byte("write-tree error"), errors.New("write-tree failure")
		}
		return []byte(""), nil
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "write transaction tree") {
		t.Fatalf("gitCommitPaths: want write-tree error, got %v", err)
	}

	// line 126-128: update-ref error
	_ = os.WriteFile(filepath.Join(tempDir, "f.txt"), []byte("data"), 0o644)
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "update-ref" {
			return []byte("update-ref error"), errors.New("update-ref failure")
		}
		return origGitCmdRun(ctx, dir, env, args...)
	}
	err = gitCommitPaths(context.Background(), tempDir, []string{filepath.Join(tempDir, "f.txt")}, "msg")
	if err == nil || !strings.Contains(err.Error(), "publish transaction commit") {
		t.Fatalf("gitCommitPaths: want update-ref error, got %v", err)
	}
}

func TestAccessManifest_Gaps(t *testing.T) {
	root := t.TempDir()

	// line 33: osLstat error on .ingitdb
	origLstat := osLstat
	defer func() { osLstat = origLstat }()
	osLstat = func(name string) (os.FileInfo, error) {
		if strings.HasSuffix(name, ".ingitdb") {
			return nil, errors.New("stat ingit error")
		}
		return origLstat(name)
	}
	_, _, err := readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "inspect access configuration directory") {
		t.Fatalf("readAccessManifest: want inspect access dir error, got %v", err)
	}
	osLstat = origLstat

	// line 39: .ingitdb is a file
	ingitFile := filepath.Join(root, ".ingitdb")
	_ = os.WriteFile(ingitFile, []byte("file"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("readAccessManifest: want must be a directory error, got %v", err)
	}
	_ = os.Remove(ingitFile)

	// line 49: osLstat error on manifest.yaml
	accessDir := filepath.Join(root, accessConfigDir)
	_ = os.MkdirAll(accessDir, 0o755)
	osLstat = func(name string) (os.FileInfo, error) {
		if strings.HasSuffix(name, accessManifestName) {
			return nil, errors.New("stat manifest error")
		}
		return origLstat(name)
	}
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "inspect access manifest") {
		t.Fatalf("readAccessManifest: want inspect access manifest error, got %v", err)
	}
	osLstat = origLstat

	// line 55: manifest.yaml is a directory
	_ = os.MkdirAll(filepath.Join(accessDir, accessManifestName), 0o755)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "must be a regular file") {
		t.Fatalf("readAccessManifest: want must be a regular file error, got %v", err)
	}
	_ = os.RemoveAll(filepath.Join(accessDir, accessManifestName))

	// line 58: manifest size exceeds maxAccessManifestSize
	manifestPath := filepath.Join(accessDir, accessManifestName)
	bigData := make([]byte, maxAccessManifestSize+10)
	_ = os.WriteFile(manifestPath, bigData, 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("readAccessManifest: want exceeds size error, got %v", err)
	}

	// line 62: osReadFile error
	origReadFile := osReadFile
	defer func() { osReadFile = origReadFile }()
	_ = os.WriteFile(manifestPath, []byte("enabled: true\n"), 0o644)
	osReadFile = func(name string) ([]byte, error) {
		if strings.HasSuffix(name, accessManifestName) {
			return nil, errors.New("readfile manifest error")
		}
		return origReadFile(name)
	}
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "read access manifest") {
		t.Fatalf("readAccessManifest: want read access manifest error, got %v", err)
	}

	// line 65: osReadFile returns > maxAccessManifestSize
	osReadFile = func(name string) ([]byte, error) {
		if strings.HasSuffix(name, accessManifestName) {
			return make([]byte, maxAccessManifestSize+10), nil
		}
		return origReadFile(name)
	}
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("readAccessManifest: want exceeds size error after read, got %v", err)
	}
	osReadFile = origReadFile

	// line 94: multiple YAML documents
	_ = os.WriteFile(manifestPath, []byte("enabled: true\n---\nextra: doc\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("readAccessManifest: want multiple YAML docs error, got %v", err)
	}

	// line 98: enabled is nil
	_ = os.WriteFile(manifestPath, []byte("database: test\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "explicit enabled") {
		t.Fatalf("readAccessManifest: want explicit enabled error, got %v", err)
	}

	// line 104: invalid generation revision
	_ = os.WriteFile(manifestPath, []byte("enabled: true\ngeneration: not-a-sha\npolicies: [p.yaml]\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "invalid access generation revision") {
		t.Fatalf("readAccessManifest: want invalid generation revision error, got %v", err)
	}

	// line 109: generation directory missing
	validSHA := strings.Repeat("a", 64)
	_ = os.WriteFile(manifestPath, []byte("enabled: true\ngeneration: "+validSHA+"\npolicies: [p.yaml]\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "verify active access generation") {
		t.Fatalf("readAccessManifest: want verify error, got %v", err)
	}

	// line 100: active manifest does not match its generation
	origVerifyGen := readAndVerifyGenerationSeam
	defer func() { readAndVerifyGenerationSeam = origVerifyGen }()
	readAndVerifyGenerationSeam = func(base, revision string) (generationManifest, error) {
		return generationManifest{
			Enabled:  false, // mismatch!
			Database: "test",
			Policies: []generationPolicy{{ID: "p1"}},
		}, nil
	}
	_ = os.WriteFile(manifestPath, []byte("enabled: true\ndatabase: test\ngeneration: "+validSHA+"\npolicies: [generations/"+validSHA+"/policies/p1.yaml]\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "active manifest does not match its generation") {
		t.Fatalf("readAccessManifest: want manifest mismatch error, got %v", err)
	}

	// line 106: active generation policy path is outside its generation
	readAndVerifyGenerationSeam = func(base, revision string) (generationManifest, error) {
		return generationManifest{
			Enabled:  true,
			Database: "test",
			Policies: []generationPolicy{{ID: "p1"}},
		}, nil
	}
	_ = os.WriteFile(manifestPath, []byte("enabled: true\ndatabase: test\ngeneration: "+validSHA+"\npolicies: [other/path.yaml]\n"), 0o644)
	_, _, err = readAccessManifest(root)
	if err == nil || !strings.Contains(err.Error(), "outside its generation") {
		t.Fatalf("readAccessManifest: want policy outside generation error, got %v", err)
	}
	readAndVerifyGenerationSeam = origVerifyGen
}
