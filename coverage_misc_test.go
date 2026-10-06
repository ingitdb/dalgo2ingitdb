package dalgo2ingitdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	dalrecord "github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/config"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
	"gopkg.in/yaml.v3"
)

func TestRecordIO_WindowsReservedSeam(t *testing.T) {
	orig := runtimeGOOS
	runtimeGOOS = "windows"
	defer func() { runtimeGOOS = orig }()

	err := validateRecordPathSegment("CON")
	if err == nil {
		t.Fatal("validateRecordPathSegment: want error for CON on windows")
	}
}

func TestRootedFilesUnix_FlockError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "flock-*")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close() // closed fd triggers flock error (EBADF)

	err = withRootedExclusiveFileLockContext(context.Background(), f, func() error { return nil })
	if err == nil {
		t.Fatal("withRootedExclusiveFileLockContext: want error on closed fd")
	}
}

func TestFilelock_FailedLockerAndSidecar(t *testing.T) {
	origLock := lockFilePathSeam
	origAbs := filepathAbs
	defer func() {
		lockFilePathSeam = origLock
		filepathAbs = origAbs
	}()

	lockFilePathSeam = func(string) (string, error) {
		return "", errors.New("lock error")
	}
	locker := defaultFileLocker("some/target")
	if err := locker.Lock(); err == nil {
		t.Fatal("Lock: want error")
	}
	if err := locker.RLock(); err == nil {
		t.Fatal("RLock: want error")
	}
	if err := locker.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	filepathAbs = func(string) (string, error) {
		return "", errors.New("abs error")
	}
	_, err := sidecarLockPath(t.TempDir(), "target", false)
	if err == nil {
		t.Fatal("sidecarLockPath: want error when filepathAbs fails")
	}
}

func TestRecordset_BuildStoredOnlyRecordset(t *testing.T) {
	t.Parallel()
	colDef := &ingitdb.CollectionDef{
		ID: "items",
		Columns: map[string]*ingitdb.ColumnDef{
			"stored_col":  {Type: ingitdb.ColumnTypeString},
			"formula_col": {Type: ingitdb.ColumnTypeString, Formula: "1 + 1"},
		},
	}
	records := []KeyedStored{
		{Key: "k1", Stored: map[string]any{"stored_col": "val1"}},
	}
	rs := buildStoredOnlyRecordset(colDef, records)
	if rs == nil {
		t.Fatal("buildStoredOnlyRecordset returned nil")
	}
}

func TestRegistry_WriteRootCollectionsYAML_Gaps(t *testing.T) {
	// Test empty dirPath -> "."
	root := t.TempDir()
	curDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(curDir) }()

	if err := writeRootCollectionsYAML("", map[string]string{"col": "path"}); err != nil {
		t.Fatalf("writeRootCollectionsYAML with empty dir: %v", err)
	}

	// Test MkdirAll error when dirPath is a file
	filePath := filepath.Join(root, "regular-file")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeRootCollectionsYAML(filePath, map[string]string{"col": "path"}); err == nil {
		t.Fatal("writeRootCollectionsYAML: want error when mkdir fails")
	}

	// Test yamlMarshal error
	origMarshal := yamlMarshal
	yamlMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal error") }
	defer func() { yamlMarshal = origMarshal }()
	if err := writeRootCollectionsYAML(root, map[string]string{"col": "path"}); err == nil {
		t.Fatal("writeRootCollectionsYAML: want error when yamlMarshal fails")
	}
	yamlMarshal = origMarshal

	// Test os.WriteFile error when target is a directory
	subRoot := t.TempDir()
	cfgDir := filepath.Join(subRoot, config.IngitDBDirName)
	_ = os.MkdirAll(cfgDir, 0o755)
	targetDir := filepath.Join(cfgDir, config.RootCollectionsFileName)
	if err := os.Mkdir(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeRootCollectionsYAML(subRoot, map[string]string{"col": "path"}); err == nil {
		t.Fatal("writeRootCollectionsYAML: want error when WriteFile fails on directory")
	}
}

func TestSchemaModifier_SubCollection_Gaps(t *testing.T) {
	root := t.TempDir()
	db, err := NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	mod, _ := dal.As[ddl.SchemaModifier](db)

	// First create parent collection
	parentCol := dbschema.CollectionDef{Name: "parent", Fields: []dbschema.FieldDef{{Name: "id", Type: dbschema.String}}}
	if err := mod.CreateCollection(context.Background(), parentCol); err != nil {
		t.Fatalf("CreateCollection parent: %v", err)
	}

	// Line 359: invalid field type in subcollection
	badCol := dbschema.CollectionDef{
		Name:   "parent/child",
		Fields: []dbschema.FieldDef{{Name: "f", Type: dbschema.Type(127)}},
	}
	if err := mod.CreateCollection(context.Background(), badCol); err == nil {
		t.Fatal("CreateCollection: want error on invalid field type")
	}

	// Line 381: indexes declaration logged
	subColWithIndex := dbschema.CollectionDef{
		Name:    "parent/child_with_idx",
		Fields:  []dbschema.FieldDef{{Name: "id", Type: dbschema.String}},
		Indexes: []dbschema.IndexDef{{Name: "idx1"}},
	}
	if err := mod.CreateCollection(context.Background(), subColWithIndex); err != nil {
		t.Fatalf("CreateCollection with indexes: %v", err)
	}

	// Line 378: osMkdirAll fails
	origMkdir := osMkdirAll
	osMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir failed") }
	defer func() { osMkdirAll = origMkdir }()

	subColMkdir := dbschema.CollectionDef{
		Name:   "parent/child_mkdir_fail",
		Fields: []dbschema.FieldDef{{Name: "id", Type: dbschema.String}},
	}
	if err := mod.CreateCollection(context.Background(), subColMkdir); err == nil {
		t.Fatal("CreateCollection: want error when osMkdirAll fails")
	}
	osMkdirAll = origMkdir

	// Line 374: stat error other than fs.ErrNotExist
	// Create parent2, then make its subcollections a file so stat returns ENOTDIR
	parent2Col := dbschema.CollectionDef{Name: "parent2", Fields: []dbschema.FieldDef{{Name: "id", Type: dbschema.String}}}
	if err := mod.CreateCollection(context.Background(), parent2Col); err != nil {
		t.Fatalf("CreateCollection parent2: %v", err)
	}
	subcolPath := filepath.Join(root, "parent2", ingitdb.SchemaDir, "subcollections")
	if err := os.WriteFile(subcolPath, []byte("file-blocking-dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	subColStat := dbschema.CollectionDef{
		Name:   "parent2/child_stat_fail",
		Fields: []dbschema.FieldDef{{Name: "id", Type: dbschema.String}},
	}
	if err := mod.CreateCollection(context.Background(), subColStat); err == nil {
		t.Fatal("CreateCollection: want error when subcollections is a file")
	}
}

func TestWriteValidation_Gaps(t *testing.T) {
	t.Parallel()
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{
			"parents": {
				ID: "parents",
				RecordFile: &ingitdb.RecordFileDef{
					RecordType: ingitdb.SingleRecord,
					Format:     ingitdb.RecordFormatYAML,
					Name:       "{key}.yaml",
				},
				Columns: map[string]*ingitdb.ColumnDef{"name": {Type: ingitdb.ColumnTypeString}},
			},
			"children": {
				ID: "children",
				RecordFile: &ingitdb.RecordFileDef{
					RecordType: ingitdb.SingleRecord,
					Format:     ingitdb.RecordFormatYAML,
					Name:       "{key}.yaml",
				},
				Columns: map[string]*ingitdb.ColumnDef{
					"parent_id": {
						Type:       ingitdb.ColumnTypeString,
						ForeignKey: "parents",
					},
				},
			},
		},
	}

	// ValidateWrite foreign key failure -> line 26
	colDef := def.Collections["children"]
	err := ValidateWrite(def, "Insert", "children", colDef, "c1", map[string]any{"parent_id": "nonexistent"})
	if err == nil {
		t.Fatal("ValidateWrite: want foreign key error")
	}

	// ValidateDelete foreign key check error -> line 40
	err = ValidateDelete(def, "parents", "p1")
	if err != nil {
		t.Fatalf("ValidateDelete: %v", err)
	}

	// ValidateDelete with nil def -> line 39/40 error
	err = ValidateDelete(nil, "parents", "p1")
	if err == nil {
		t.Fatal("ValidateDelete with nil def: want error")
	}
}

type badTypeStruct struct {
	Title int `json:"title"`
}

func TestTxReadonly_Gaps(t *testing.T) {
	root := t.TempDir()
	db, err := NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	mod, _ := dal.As[ddl.SchemaModifier](db)
	err = mod.CreateCollection(context.Background(), dbschema.CollectionDef{
		Name: "single_items",
		Fields: []dbschema.FieldDef{
			{Name: "title", Type: dbschema.String},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	key := dalrecord.NewKeyWithID("single_items", "item1")
	rec := dalrecord.NewRecordWithData(key, map[string]any{"title": "not-an-int"})
	err = db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, rec)
	})
	if err != nil {
		t.Fatalf("Insert single_item: %v", err)
	}

	// Get with incompatible struct data type -> line 73-75
	badTarget := dalrecord.NewRecordWithData(key, &badTypeStruct{})
	err = db.Get(context.Background(), badTarget)
	if err == nil {
		t.Fatal("Get: want error on type mismatch")
	}

	// MapOfRecords incompatible type -> line 96-98
	def, err := dal.BackendOf(db).(*Database).loadDefinition()
	if err != nil {
		t.Fatal(err)
	}
	mapColDef := &ingitdb.CollectionDef{
		ID:      "map_items",
		DirPath: root,
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.MapOfRecords,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "map.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"title": {Type: ingitdb.ColumnTypeString}},
	}
	_ = os.WriteFile(filepath.Join(root, "map.yaml"), []byte("k1:\n  title: not-an-int\n"), 0o644)
	def.Collections["map_items"] = mapColDef
	mapKey := dalrecord.NewKeyWithID("map_items", "k1")
	mapTarget := dalrecord.NewRecordWithData(mapKey, &badTypeStruct{})
	rTxMap := readonlyTx{def: def}
	err = rTxMap.Get(context.Background(), mapTarget)
	if err == nil {
		t.Fatal("Get on MapOfRecords: want error on type mismatch")
	}

	// GetMulti with canceled context -> line 146
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.GetMulti(ctx, []dalrecord.Record{rec})
	if err == nil {
		t.Fatal("GetMulti: want error on canceled context")
	}

	// ExecuteQueryToRecordsetReader with storedOnlyReads = true -> line 186-187
	def, err = dal.BackendOf(db).(*Database).loadDefinition()
	if err != nil {
		t.Fatal(err)
	}
	rTx := readonlyTx{def: def, db: &Database{storedOnlyReads: true}}
	q := dal.From(dal.NewRootCollectionRef("single_items", "")).NewQuery().
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("single_items", ""), map[string]any{})
		})
	reader, err := rTx.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsetReader: %v", err)
	}
	if reader == nil {
		t.Fatal("reader is nil")
	}
	_ = reader.Close()
}

func TestScopedCollection_Gaps(t *testing.T) {
	// Line 41: def == nil
	_, err := resolveScopedCollection(nil, "col", nil)
	if err == nil {
		t.Fatal("resolveScopedCollection: want error on nil def")
	}

	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{
			"root_col": {
				ID:      "root_col",
				DirPath: "/root",
				SubCollections: map[string]*ingitdb.CollectionDef{
					"mid_col": {
						ID:      "mid_col",
						DirPath: "/root/mid_col",
						SubCollections: map[string]*ingitdb.CollectionDef{
							"leaf_col": {
								ID:      "leaf_col",
								DirPath: "/root/mid_col/leaf_col",
							},
						},
					},
				},
			},
		},
	}

	// Line 69: rootCol not in def.Collections
	badRootKey := dalrecord.NewKeyWithID("unknown_root", "id1")
	_, err = resolveScopedCollection(def, "leaf_col", badRootKey)
	if err == nil {
		t.Fatal("resolveScopedCollection: want error on unknown root col")
	}

	// Line 84: intermediate subcollection missing
	// Chain: root_col/r1 -> missing_mid/m1 -> target
	keyRoot := dalrecord.NewKeyWithID("root_col", "r1")
	keyMissingMid := dalrecord.NewKeyWithParentAndID(keyRoot, "missing_mid", "m1")
	_, err = resolveScopedCollection(def, "leaf_col", keyMissingMid)
	if err == nil {
		t.Fatal("resolveScopedCollection: want error on missing intermediate subcollection")
	}

	// Line 80-87: intermediate subcollection exists
	keyMid := dalrecord.NewKeyWithParentAndID(keyRoot, "mid_col", "m1")
	colDef, err := resolveScopedCollection(def, "leaf_col", keyMid)
	if err != nil {
		t.Fatalf("resolveScopedCollection with mid: %v", err)
	}
	if colDef == nil || colDef.ID != "leaf_col" {
		t.Fatalf("unexpected colDef: %+v", colDef)
	}

	// Line 95: target subcollection not under cur
	_, err = resolveScopedCollection(def, "nonexistent_leaf", keyMid)
	if err == nil {
		t.Fatal("resolveScopedCollection: want error on missing target leaf")
	}

	// Line 99: filepathRel fails in requireContainedPath
	origRel := filepathRel
	filepathRel = func(string, string) (string, error) { return "", errors.New("rel failure") }
	defer func() { filepathRel = origRel }()
	_, err = resolveScopedCollection(def, "leaf_col", keyMid)
	if err == nil {
		t.Fatal("resolveScopedCollection: want error when filepathRel fails")
	}
}

func sampleDTQLPolicy(name, db string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: %s}
target: {database: %s}
composition: dalgo-hierarchical-v1
default: deny
scopes:
  - path: /p
    rules:
      - {id: r, effect: allow, operations: [get]}
`, name, db))
}

func TestAccessGeneration_ControllerAndRevision_Gaps(t *testing.T) {
	// NewOwnerPolicyController error when directory does not exist (line 53)
	if _, err := NewOwnerPolicyController(filepath.Join(t.TempDir(), "nonexistent")); err == nil {
		t.Fatal("NewOwnerPolicyController want error for missing dir")
	}

	dir := t.TempDir()
	c, err := NewOwnerPolicyController(dir)
	if err != nil {
		t.Fatal(err)
	}

	// ActiveRevision (line 92)
	_, _ = c.ActiveRevision(context.Background())

	// Reload lock error (line 63)
	origTx := transactionLockPathSeam
	defer func() { transactionLockPathSeam = origTx }()
	transactionLockPathSeam = func(ctx context.Context, projectPath string) (string, error) {
		return "", errors.New("lock fail")
	}
	if _, err := c.Reload(context.Background()); err == nil {
		t.Fatal("c.Reload want lock error")
	}
	transactionLockPathSeam = origTx

	// Reload with readAccessManifest error (line 72)
	accessDir := filepath.Join(dir, accessConfigDir)
	_ = os.MkdirAll(accessDir, 0o755)
	manifestFile := filepath.Join(accessDir, accessManifestName)
	_ = os.WriteFile(manifestFile, []byte("invalid: : : yaml"), 0o644)
	if _, err := c.Reload(context.Background()); err == nil {
		t.Fatal("c.Reload want readAccessManifest error")
	}

	// Reload not present / !config.Enabled (line 75)
	_ = os.WriteFile(manifestFile, []byte("enabled: false\n"), 0o644)
	if _, err := c.Reload(context.Background()); err == nil || !strings.Contains(err.Error(), "no enabled owner policy generation") {
		t.Fatalf("c.Reload want not enabled error, got %v", err)
	}

	// Reload revision == "" (line 79)
	_ = os.WriteFile(manifestFile, []byte("enabled: true\npolicies:\n  - p.yaml\n"), 0o644)
	if _, err := c.Reload(context.Background()); err == nil || !strings.Contains(err.Error(), "not committed") {
		t.Fatalf("c.Reload want not committed error, got %v", err)
	}

	// Reload accessLoadPolicyFiles error (line 83)
	sha := strings.Repeat("f", 64)
	origRevAt := committedGenerationRevisionAtSeam
	origGenSeam := readAndVerifyGenerationSeam
	origLoad := accessLoadPolicyFiles
	defer func() {
		committedGenerationRevisionAtSeam = origRevAt
		readAndVerifyGenerationSeam = origGenSeam
		accessLoadPolicyFiles = origLoad
	}()
	committedGenerationRevisionAtSeam = func(ctx context.Context, root, ref string) (string, error) {
		return sha, nil
	}
	readAndVerifyGenerationSeam = func(base, revision string) (generationManifest, error) {
		return generationManifest{
			Enabled:  true,
			Policies: []generationPolicy{{ID: "p1"}},
		}, nil
	}
	accessLoadPolicyFiles = func(base string, cfg access.FilePolicyConfig) ([]access.Policy, error) {
		return nil, errors.New("load policies fail")
	}
	manifestWithGen := fmt.Sprintf("enabled: true\ngeneration: %s\npolicies:\n  - generations/%s/policies/p1.yaml\n", sha, sha)
	_ = os.WriteFile(manifestFile, []byte(manifestWithGen), 0o644)
	if _, err := c.Reload(context.Background()); err == nil || !strings.Contains(err.Error(), "load policies fail") {
		t.Fatalf("c.Reload want load policies error, got %v", err)
	}
	committedGenerationRevisionAtSeam = origRevAt
	readAndVerifyGenerationSeam = origGenSeam
	accessLoadPolicyFiles = origLoad

	// workingGenerationRevision gaps (lines 98, 101, 105, 108)
	emptyDir := t.TempDir()
	rev, err := workingGenerationRevision(emptyDir)
	if err != nil || rev != "" {
		t.Fatalf("workingGenerationRevision not exist: %v, %v", rev, err)
	}

	badDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(badDir, accessConfigDir, accessManifestName), 0o755)
	if _, err := workingGenerationRevision(badDir); err == nil {
		t.Fatal("workingGenerationRevision want read error for directory")
	}

	badYAMLDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(badYAMLDir, accessConfigDir), 0o755)
	_ = os.WriteFile(filepath.Join(badYAMLDir, accessConfigDir, accessManifestName), []byte(":::invalid"), 0o644)
	rev, err = workingGenerationRevision(badYAMLDir)
	if err != nil || rev != "" {
		t.Fatalf("workingGenerationRevision bad yaml want empty, got %v, %v", rev, err)
	}

	badSHADir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(badSHADir, accessConfigDir), 0o755)
	_ = os.WriteFile(filepath.Join(badSHADir, accessConfigDir, accessManifestName), []byte("generation: not-sha256\n"), 0o644)
	if _, err := workingGenerationRevision(badSHADir); err == nil || !strings.Contains(err.Error(), "invalid working access generation") {
		t.Fatalf("workingGenerationRevision want invalid sha error, got %v", err)
	}
}

func TestAccessGeneration_RecoverCommitted_Gaps(t *testing.T) {
	dir := t.TempDir()
	origGitCmd := gitCmdRun
	origGitBlob := gitShowBlob
	origTx := transactionLockPathSeam
	defer func() {
		gitCmdRun = origGitCmd
		gitShowBlob = origGitBlob
		transactionLockPathSeam = origTx
	}()

	// line 118: not inside git work tree returns nil
	if err := recoverCommittedGeneration(context.Background(), dir); err != nil {
		t.Fatalf("recoverCommittedGeneration non-git want nil, got %v", err)
	}

	cmd := exec.Command("git", "-C", dir, "init")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %s", out)
	}

	transactionLockPathSeam = func(ctx context.Context, projectPath string) (string, error) {
		return "", errors.New("tx lock error")
	}
	if err := recoverCommittedGeneration(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "tx lock error") {
		t.Fatalf("recoverCommittedGeneration want tx lock error, got %v", err)
	}
	transactionLockPathSeam = origTx

	// 1. decoder error / empty generation (line 139)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if strings.HasSuffix(path, accessManifestName) {
			return []byte("unknown_field: 123\n"), nil
		}
		return nil, errors.New("not found")
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err != nil {
		t.Fatalf("recoverCommittedGenerationLocked want nil on unmarshal fail, got %v", err)
	}

	// 2. !isSHA256(active.Generation) (line 142)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if strings.HasSuffix(path, accessManifestName) {
			return []byte("generation: bad-sha\n"), nil
		}
		return nil, errors.New("not found")
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("recoverCommittedGenerationLocked want invalid sha error, got %v", err)
	}

	sha := strings.Repeat("a", 64)
	manifestData := []byte("generation: " + sha + "\n")

	manifestPath := filepath.ToSlash(filepath.Join(accessConfigDir, accessManifestName))

	// 3. gitBlob genPath error (line 147)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return manifestData, nil
		}
		return nil, errors.New("blob genPath fail")
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "read committed generation") {
		t.Fatalf("recoverCommittedGenerationLocked want read committed error, got %v", err)
	}

	// 4. sha256Text mismatch (line 150)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return manifestData, nil
		}
		return []byte("dummy bytes"), nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("recoverCommittedGenerationLocked want digest mismatch, got %v", err)
	}

	// 5. yaml.Unmarshal(genBytes) error (line 154)
	invalidYAMLGen := []byte("::not yaml")
	invalidYAMLSha := sha256Text(invalidYAMLGen)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + invalidYAMLSha + "\n"), nil
		}
		return invalidYAMLGen, nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil {
		t.Fatal("recoverCommittedGenerationLocked want yaml error")
	}

	// 6. unsafe policy reference (line 159)
	badPolGen := generationManifest{
		APIVersion: generationAPIVersion,
		Policies: []generationPolicy{
			{ID: "invalid/id", File: "policies/invalid/id.yaml"},
		},
	}
	badPolBytes, _ := yaml.Marshal(badPolGen)
	badPolSha := sha256Text(badPolBytes)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + badPolSha + "\n"), nil
		}
		return badPolBytes, nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "unsafe committed policy reference") {
		t.Fatalf("recoverCommittedGenerationLocked want unsafe policy error, got %v", err)
	}

	// 7. gitBlob policy error (line 164)
	doc, err := access.ParseDTQLPolicy(sampleDTQLPolicy("p1", "db1"))
	if err != nil {
		t.Fatal(err)
	}
	docYAML, _ := access.MarshalDTQLPolicyYAML(doc)
	docJSON, _ := access.MarshalDTQLPolicyJSON(doc)
	docDigest := sha256Text(docJSON)

	goodGen := generationManifest{
		APIVersion: generationAPIVersion,
		Policies: []generationPolicy{
			{ID: "p1", File: "policies/p1.yaml", Digest: docDigest},
		},
	}
	goodGenBytes, _ := yaml.Marshal(goodGen)
	goodGenSha := sha256Text(goodGenBytes)

	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + goodGenSha + "\n"), nil
		}
		if strings.HasSuffix(path, "manifest.yaml") {
			return goodGenBytes, nil
		}
		return nil, errors.New("blob policy read error")
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "blob policy read error") {
		t.Fatalf("recoverCommittedGenerationLocked want policy blob error, got %v", err)
	}

	// 8. ParseDTQLPolicy error (line 168)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + goodGenSha + "\n"), nil
		}
		if strings.HasSuffix(path, "manifest.yaml") {
			return goodGenBytes, nil
		}
		return []byte("not dtql policy"), nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil {
		t.Fatal("recoverCommittedGenerationLocked want ParseDTQLPolicy error")
	}

	// 9. policy mismatch (line 172)
	docBadName := doc
	docBadName.Metadata.Name = "diff_name"
	docBadYAML, _ := access.MarshalDTQLPolicyYAML(docBadName)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + goodGenSha + "\n"), nil
		}
		if strings.HasSuffix(path, "manifest.yaml") {
			return goodGenBytes, nil
		}
		return docBadYAML, nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("recoverCommittedGenerationLocked want mismatch error, got %v", err)
	}

	// 10. dirty access manifest is not recoverable (line 181)
	workingManifest := filepath.Join(dir, accessConfigDir, accessManifestName)
	_ = os.MkdirAll(filepath.Dir(workingManifest), 0o755)
	_ = os.WriteFile(workingManifest, []byte("generation: dirty-not-sha\n"), 0o644)
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		if path == manifestPath {
			return []byte("generation: " + goodGenSha + "\n"), nil
		}
		if strings.HasSuffix(path, "manifest.yaml") {
			return goodGenBytes, nil
		}
		return docYAML, nil
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "dirty access manifest") {
		t.Fatalf("recoverCommittedGenerationLocked want dirty error, got %v", err)
	}

	// 11. working manifest readErr is non-NotExist (line 184)
	_ = os.Remove(workingManifest)
	_ = os.Mkdir(workingManifest, 0o755)
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil {
		t.Fatal("recoverCommittedGenerationLocked want readErr non-notexist")
	}
	_ = os.Remove(workingManifest)

	// 12. generationBase not a safe dir (line 189)
	genBaseDir := filepath.Join(dir, accessConfigDir, "generations", goodGenSha)
	_ = os.MkdirAll(filepath.Dir(genBaseDir), 0o755)
	_ = os.WriteFile(genBaseDir, []byte("file-not-dir"), 0o644)
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "safe directory") {
		t.Fatalf("recoverCommittedGenerationLocked want safe directory error, got %v", err)
	}
	_ = os.Remove(genBaseDir)

	// 13. working file dirty or corrupt (line 197)
	_ = os.MkdirAll(genBaseDir, 0o755)
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "dirty or corrupt") {
		t.Fatalf("recoverCommittedGenerationLocked want dirty or corrupt error, got %v", err)
	}
	_ = os.RemoveAll(genBaseDir)

	// 14. statErr is ErrNotExist -> calls materializeCommittedGeneration (lines 198-199)
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err != nil {
		t.Fatalf("recoverCommittedGenerationLocked with materialize want success, got %v", err)
	}

	// 14b. materializeCommittedGeneration error in recoverCommittedGenerationLocked (line 199)
	_ = os.RemoveAll(filepath.Join(dir, accessConfigDir, "generations"))
	syncCount := 0
	origSync := syncDirSeam
	defer func() { syncDirSeam = origSync }()
	syncDirSeam = func(name string) error {
		if strings.Contains(name, ".generation-") {
			syncCount++
			if syncCount > 2 {
				return errors.New("materialize fail")
			}
		}
		return syncDir(name)
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "materialize fail") {
		t.Fatalf("recoverCommittedGenerationLocked want materialize fail, got %v", err)
	}
	syncDirSeam = origSync

	// Re-materialize so test 15 has generationBase:
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err != nil {
		t.Fatalf("recoverCommittedGenerationLocked re-materialize want success, got %v", err)
	}

	// 15. statErr is non-NotExist (line 202)
	origLstat := osLstat
	defer func() { osLstat = origLstat }()
	osLstat = func(name string) (os.FileInfo, error) {
		if strings.Contains(name, goodGenSha) {
			return nil, errors.New("lstat failure")
		}
		return os.Lstat(name)
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "lstat failure") {
		t.Fatalf("recoverCommittedGenerationLocked want lstat failure, got %v", err)
	}
	osLstat = origLstat

	// 16. rejectSymlinkAncestors error (line 208)
	working := filepath.Join(dir, accessConfigDir, accessManifestName)
	_ = os.Remove(working)
	origLstat = osLstat
	defer func() { osLstat = origLstat }()
	osLstat = func(name string) (os.FileInfo, error) {
		if name == filepath.Join(dir, accessConfigDir) {
			return nil, errors.New("symlink check error")
		}
		return os.Lstat(name)
	}
	if err := recoverCommittedGenerationLocked(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "symlink check error") {
		t.Fatalf("recoverCommittedGenerationLocked want symlink error, got %v", err)
	}
	osLstat = origLstat
}

func TestAccessGeneration_MaterializeCommitted_And_Symlink_Gaps(t *testing.T) {
	dir := t.TempDir()
	rev := strings.Repeat("b", 64)
	blobs := map[string][]byte{
		filepath.ToSlash(filepath.Join(accessConfigDir, "generations", rev, "manifest.yaml")): []byte("manifest"),
	}

	// 1. rejectSymlinkAncestors error (line 216)
	_ = os.RemoveAll(filepath.Join(dir, ".ingitdb"))
	_ = os.Symlink(t.TempDir(), filepath.Join(dir, ".ingitdb"))
	if err := materializeCommittedGeneration(dir, rev, blobs); err == nil || !strings.Contains(err.Error(), "symlink ancestor") {
		t.Fatalf("materializeCommittedGeneration want symlink error, got %v", err)
	}
	_ = os.Remove(filepath.Join(dir, ".ingitdb"))

	// 2. os.MkdirAll error (line 219)
	parent := filepath.Join(dir, accessConfigDir, "generations")
	_ = os.MkdirAll(filepath.Dir(parent), 0o755)
	_ = os.WriteFile(parent, []byte("file-not-dir"), 0o644)
	if err := materializeCommittedGeneration(dir, rev, blobs); err == nil {
		t.Fatal("materializeCommittedGeneration want MkdirAll error")
	}
	_ = os.Remove(parent)

	// 3. os.MkdirTemp error (line 223)
	_ = os.MkdirAll(parent, 0o755)
	origMkdirTemp := osMkdirTemp
	osMkdirTemp = func(string, string) (string, error) { return "", errors.New("injected MkdirTemp failure") }
	if err := materializeCommittedGeneration(dir, rev, blobs); err == nil {
		t.Fatal("materializeCommittedGeneration want MkdirTemp error")
	}
	osMkdirTemp = origMkdirTemp

	// 4. committed generation path escaped (line 230)
	badBlobs := map[string][]byte{"escaped/path.yaml": []byte("data")}
	if err := materializeCommittedGeneration(dir, rev, badBlobs); err == nil || !strings.Contains(err.Error(), "path escaped") {
		t.Fatalf("materializeCommittedGeneration want path escaped error, got %v", err)
	}

	// 5. atomicWriteFile error (line 233)
	longPath := filepath.ToSlash(filepath.Join(accessConfigDir, "generations", rev, strings.Repeat("a", 300)))
	if err := materializeCommittedGeneration(dir, rev, map[string][]byte{longPath: []byte("data")}); err == nil {
		t.Fatal("materializeCommittedGeneration want atomicWriteFile error")
	}

	// 5b. syncDir(tmp) error (line 237)
	syncCount := 0
	origSync := syncDirSeam
	defer func() { syncDirSeam = origSync }()
	syncDirSeam = func(name string) error {
		if strings.Contains(name, ".generation-") {
			syncCount++
			if syncCount > 1 {
				return errors.New("sync tmp fail")
			}
		}
		return syncDir(name)
	}
	if err := materializeCommittedGeneration(dir, rev, blobs); err == nil || !strings.Contains(err.Error(), "sync tmp fail") {
		t.Fatalf("materializeCommittedGeneration want sync tmp fail, got %v", err)
	}
	syncDirSeam = origSync

	// 6. osRename error (line 241)
	origRename := osRename
	defer func() { osRename = origRename }()
	osRename = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, rev) {
			return errors.New("rename fail")
		}
		return os.Rename(oldpath, newpath)
	}
	if err := materializeCommittedGeneration(dir, rev, blobs); err == nil || !strings.Contains(err.Error(), "rename fail") {
		t.Fatalf("materializeCommittedGeneration want rename error, got %v", err)
	}
	osRename = origRename

	// 7. success path (line 243)
	if err := materializeCommittedGeneration(dir, rev, blobs); err != nil {
		t.Fatalf("materializeCommittedGeneration want success, got %v", err)
	}

	// rejectSymlinkAncestors tests:
	if err := rejectSymlinkAncestors(dir, filepath.Join(dir, "..", "outside")); err == nil || !strings.Contains(err.Error(), "escapes project") {
		t.Fatalf("rejectSymlinkAncestors want escapes project error, got %v", err)
	}
	if err := rejectSymlinkAncestors(dir, filepath.Join(dir, "does_not_exist")); err != nil {
		t.Fatalf("rejectSymlinkAncestors not exist want nil, got %v", err)
	}
	origLstat := osLstat
	defer func() { osLstat = origLstat }()
	osLstat = func(name string) (os.FileInfo, error) {
		return nil, errors.New("lstat failure")
	}
	if err := rejectSymlinkAncestors(dir, filepath.Join(dir, "something")); err == nil || !strings.Contains(err.Error(), "lstat failure") {
		t.Fatalf("rejectSymlinkAncestors want lstat failure, got %v", err)
	}
	osLstat = origLstat
	symLink := filepath.Join(dir, "sym_dir")
	_ = os.Symlink(t.TempDir(), symLink)
	if err := rejectSymlinkAncestors(dir, filepath.Join(symLink, "child")); err == nil || !strings.Contains(err.Error(), "symlink ancestor") {
		t.Fatalf("rejectSymlinkAncestors want symlink ancestor error, got %v", err)
	}
}

func TestAccessGeneration_Publish_Gaps(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command("git", "-C", dir, "init")
	_ = cmd.Run()
	cmd = exec.Command("git", "-C", dir, "config", "user.email", "test@test.com")
	_ = cmd.Run()
	cmd = exec.Command("git", "-C", dir, "config", "user.name", "Test")
	_ = cmd.Run()
	_ = os.WriteFile(filepath.Join(dir, "README.md"), []byte("init"), 0o644)
	cmd = exec.Command("git", "-C", dir, "add", ".")
	_ = cmd.Run()
	cmd = exec.Command("git", "-C", dir, "commit", "-m", "init")
	_ = cmd.Run()

	c, err := NewOwnerPolicyController(dir)
	if err != nil {
		t.Fatal(err)
	}
	cand := OwnerPolicyGeneration{
		Enabled:  true,
		Database: "db1",
		Policies: []OwnerPolicyDocument{
			{YAML: sampleDTQLPolicy("p1", "db1")},
		},
	}

	// 1. transactionLockPath error (line 298)
	origTx := transactionLockPathSeam
	defer func() { transactionLockPathSeam = origTx }()
	transactionLockPathSeam = func(ctx context.Context, projectPath string) (string, error) {
		return "", errors.New("tx lock error")
	}
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "tx lock error") {
		t.Fatalf("Publish want tx lock error, got %v", err)
	}
	transactionLockPathSeam = origTx

	// 2. gitHead error (line 304)
	origHead := gitHeadSeam
	defer func() { gitHeadSeam = origHead }()
	gitHeadSeam = func(ctx context.Context, dir string) (string, bool, error) {
		return "", false, errors.New("gitHead error")
	}
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "gitHead error") {
		t.Fatalf("Publish want gitHead error, got %v", err)
	}
	gitHeadSeam = origHead

	// 2b. committedGenerationRevisionAt error (line 311)
	origRevAt := committedGenerationRevisionAtSeam
	defer func() { committedGenerationRevisionAtSeam = origRevAt }()
	committedGenerationRevisionAtSeam = func(ctx context.Context, root, ref string) (string, error) {
		return "", errors.New("rev at fail")
	}
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "rev at fail") {
		t.Fatalf("Publish want rev at fail, got %v", err)
	}
	committedGenerationRevisionAtSeam = origRevAt

	// 3. !hasHead (line 307)
	gitHeadSeam = func(ctx context.Context, dir string) (string, bool, error) {
		return "", false, nil
	}
	origGitCmd := gitCmdRun
	defer func() { gitCmdRun = origGitCmd }()
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		return nil, errors.New("stop here")
	}
	_, _ = c.Publish(context.Background(), cand, "", "msg")
	gitHeadSeam = origHead

	// 4. yamlMarshal error on activeBytes (line 336)
	origYAML := yamlMarshal
	defer func() { yamlMarshal = origYAML }()
	gitHeadSeam = func(ctx context.Context, dir string) (string, bool, error) {
		return "", false, nil
	}
	yamlMarshal = func(in any) ([]byte, error) {
		if _, ok := in.(activeGenerationManifest); ok {
			return nil, errors.New("yaml marshal fail")
		}
		return yaml.Marshal(in)
	}
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "yaml marshal fail") {
		t.Fatalf("Publish want yaml marshal error, got %v", err)
	}
	yamlMarshal = origYAML

	// 5. rejectSymlinkAncestors error (line 347)
	_ = os.RemoveAll(filepath.Join(dir, ".ingitdb"))
	_ = os.Symlink(t.TempDir(), filepath.Join(dir, ".ingitdb"))
	origGitStdin := gitCmdRunStdin
	defer func() { gitCmdRunStdin = origGitStdin }()
	gitCmdRunStdin = func(ctx context.Context, dir string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		return []byte("commit-sha\n"), nil
	}
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		return []byte("tree-sha\n"), nil
	}
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "symlink ancestor") {
		t.Fatalf("Publish want symlink ancestor error, got %v", err)
	}
	_ = os.Remove(filepath.Join(dir, ".ingitdb"))

	// 6. atomicWriteFile error (line 350)
	_ = os.MkdirAll(filepath.Join(dir, accessConfigDir, accessManifestName), 0o755)
	if _, err := c.Publish(context.Background(), cand, "", "msg"); err == nil || !strings.Contains(err.Error(), "working activation needs recovery") {
		t.Fatalf("Publish want atomicWriteFile error, got %v", err)
	}
	_ = os.RemoveAll(filepath.Join(dir, accessConfigDir))
}

func TestAccessGeneration_BuildGeneration_Gaps(t *testing.T) {
	// 1. candidate.Database == "" or !candidate.Enabled or len == 0 (line 363)
	if _, _, _, err := buildGeneration(OwnerPolicyGeneration{}); err == nil {
		t.Fatal("buildGeneration empty want error")
	}

	// 2. ParseDTQLPolicy error (line 376)
	candBadPolicy := OwnerPolicyGeneration{
		Enabled:  true,
		Database: "db1",
		Policies: []OwnerPolicyDocument{{YAML: []byte("not-yaml")}},
	}
	if _, _, _, err := buildGeneration(candBadPolicy); err == nil {
		t.Fatal("buildGeneration want ParseDTQLPolicy error")
	}

	// 3. doc.Target.Database != candidate.Database (line 383)
	candWrongDB := OwnerPolicyGeneration{
		Enabled:  true,
		Database: "db1",
		Policies: []OwnerPolicyDocument{
			{YAML: sampleDTQLPolicy("p1", "wrong_db")},
		},
	}
	if _, _, _, err := buildGeneration(candWrongDB); err == nil || !strings.Contains(err.Error(), "targets wrong database") {
		t.Fatalf("buildGeneration want wrong db error, got %v", err)
	}

	// 4. duplicate policy id (line 392)
	candDup := OwnerPolicyGeneration{
		Enabled:  true,
		Database: "db1",
		Policies: []OwnerPolicyDocument{
			{YAML: sampleDTQLPolicy("p1", "db1")},
			{YAML: sampleDTQLPolicy("p1", "db1")},
		},
	}
	if _, _, _, err := buildGeneration(candDup); err == nil || !strings.Contains(err.Error(), "duplicate policy id") {
		t.Fatalf("buildGeneration want duplicate policy error, got %v", err)
	}

	// 5. sort.Slice less condition with multiple policies (line 401)
	candMulti := OwnerPolicyGeneration{
		Enabled:  true,
		Database: "db1",
		Policies: []OwnerPolicyDocument{
			{YAML: sampleDTQLPolicy("z_policy", "db1")},
			{YAML: sampleDTQLPolicy("a_policy", "db1")},
		},
	}
	manifest, _, _, err := buildGeneration(candMulti)
	if err != nil {
		t.Fatalf("buildGeneration multi: %v", err)
	}
	if manifest.Policies[0].ID != "a_policy" {
		t.Fatalf("buildGeneration sort want a_policy first, got %v", manifest.Policies[0].ID)
	}

	// 6. yamlMarshal error on manifest (line 404)
	origYAML := yamlMarshal
	defer func() { yamlMarshal = origYAML }()
	yamlMarshal = func(in any) ([]byte, error) {
		if _, ok := in.(generationManifest); ok {
			return nil, errors.New("marshal manifest fail")
		}
		return yaml.Marshal(in)
	}
	if _, _, _, err := buildGeneration(candMulti); err == nil || !strings.Contains(err.Error(), "marshal manifest fail") {
		t.Fatalf("buildGeneration want marshal manifest fail, got %v", err)
	}

	// 7. accessMarshalDTQLPolicyJSON error (line 387)
	origJSON := accessMarshalDTQLPolicyJSON
	defer func() { accessMarshalDTQLPolicyJSON = origJSON }()
	accessMarshalDTQLPolicyJSON = func(doc access.DTQLDocument) ([]byte, error) {
		return nil, errors.New("marshal json fail")
	}
	if _, _, _, err := buildGeneration(candMulti); err == nil || !strings.Contains(err.Error(), "marshal json fail") {
		t.Fatalf("buildGeneration want marshal json fail, got %v", err)
	}
	accessMarshalDTQLPolicyJSON = origJSON

	// 8. accessMarshalDTQLPolicyYAML error (line 396)
	origYAMLSeam := accessMarshalDTQLPolicyYAML
	defer func() { accessMarshalDTQLPolicyYAML = origYAMLSeam }()
	accessMarshalDTQLPolicyYAML = func(doc access.DTQLDocument) ([]byte, error) {
		return nil, errors.New("marshal yaml fail")
	}
	if _, _, _, err := buildGeneration(candMulti); err == nil || !strings.Contains(err.Error(), "marshal yaml fail") {
		t.Fatalf("buildGeneration want marshal yaml fail, got %v", err)
	}
	accessMarshalDTQLPolicyYAML = origYAMLSeam
}

func TestAccessGeneration_MaterializeGeneration_Gaps(t *testing.T) {
	dir := t.TempDir()
	rev := strings.Repeat("c", 64)
	manifest := generationManifest{
		APIVersion: generationAPIVersion,
		Enabled:    true,
		Database:   "db1",
		Policies:   []generationPolicy{{ID: "p1", File: "policies/p1.yaml", Digest: "dig1"}},
	}
	sources := map[string][]byte{"policies/p1.yaml": []byte("policy data")}

	// 1. base exists but verifyGeneration fails (line 414)
	base := filepath.Join(dir, accessConfigDir, "generations", rev)
	_ = os.MkdirAll(base, 0o755)
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "existing generation invalid") {
		t.Fatalf("materializeGeneration want invalid existing error, got %v", err)
	}
	_ = os.RemoveAll(base)

	// 2. base exists as a regular file (line 422)
	_ = os.MkdirAll(filepath.Dir(base), 0o755)
	_ = os.WriteFile(base, []byte("file-not-dir"), 0o644)
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("materializeGeneration want non-symlink error, got %v", err)
	}
	_ = os.Remove(base)

	// 3. os.MkdirAll error on parent (line 426)
	parent := filepath.Dir(base)
	_ = os.RemoveAll(parent)
	_ = os.MkdirAll(filepath.Dir(parent), 0o755)
	_ = os.WriteFile(parent, []byte("file-blocks-parent"), 0o644)
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil {
		t.Fatal("materializeGeneration want MkdirAll error")
	}
	_ = os.Remove(parent)

	// 4. os.MkdirTemp error (line 430)
	_ = os.MkdirAll(parent, 0o755)
	origMkdirTemp := osMkdirTemp
	osMkdirTemp = func(string, string) (string, error) { return "", errors.New("injected MkdirTemp failure") }
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil {
		t.Fatal("materializeGeneration want MkdirTemp error")
	}
	osMkdirTemp = origMkdirTemp

	// 5. atomicWriteFile error on source (line 437)
	badSources := map[string][]byte{strings.Repeat("long", 100): []byte("data")}
	if _, err := materializeGeneration(dir, rev, manifest, badSources); err == nil {
		t.Fatal("materializeGeneration want atomicWriteFile source error")
	}

	// 5b. atomicWriteFile error on manifest (line 442)
	origRename := osRename
	defer func() { osRename = origRename }()
	osRename = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, "manifest.yaml") {
			return errors.New("write manifest fail")
		}
		return os.Rename(oldpath, newpath)
	}
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "write manifest fail") {
		t.Fatalf("materializeGeneration want write manifest fail, got %v", err)
	}
	osRename = origRename

	// 5c. syncDir(tmp) error (line 446)
	syncCount := 0
	origSync := syncDirSeam
	defer func() { syncDirSeam = origSync }()
	syncDirSeam = func(name string) error {
		if strings.Contains(name, ".generation-") {
			syncCount++
			if syncCount > len(sources)+1 {
				return errors.New("sync tmp fail")
			}
		}
		return syncDir(name)
	}
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "sync tmp fail") {
		t.Fatalf("materializeGeneration want sync tmp fail, got %v", err)
	}
	syncDirSeam = origSync

	// 6. osRename error (line 452)
	osRename = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, rev) {
			return errors.New("rename fail")
		}
		return os.Rename(oldpath, newpath)
	}
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "rename fail") {
		t.Fatalf("materializeGeneration want rename fail, got %v", err)
	}
	osRename = origRename

	// 7. syncDir(parent) error (line 458)
	syncDirSeam = func(name string) error {
		if strings.HasSuffix(name, "generations") {
			return errors.New("sync parent fail")
		}
		return syncDir(name)
	}
	if _, err := materializeGeneration(dir, rev, manifest, sources); err == nil || !strings.Contains(err.Error(), "sync parent fail") {
		t.Fatalf("materializeGeneration want sync parent fail, got %v", err)
	}
	syncDirSeam = origSync
}

func TestAccessGeneration_ReadAndVerifyGeneration_Gaps(t *testing.T) {
	dir := t.TempDir()

	// 1. manifestPath is a directory, not a regular file (line 479)
	manifestPath := filepath.Join(dir, "manifest.yaml")
	_ = os.Mkdir(manifestPath, 0o755)
	if _, err := readAndVerifyGeneration(dir, "any"); err == nil || !strings.Contains(err.Error(), "regular non-symlink file") {
		t.Fatalf("readAndVerifyGeneration want regular file error, got %v", err)
	}
	_ = os.Remove(manifestPath)

	// 2. manifest read error via chmod 000 (line 483)
	_ = os.WriteFile(manifestPath, []byte("data"), 0o000)
	defer func() { _ = os.Chmod(manifestPath, 0o644) }()
	if _, err := readAndVerifyGeneration(dir, "any"); err == nil {
		t.Fatal("readAndVerifyGeneration want read error")
	}
	_ = os.Chmod(manifestPath, 0o644)

	// 3. digest mismatch (line 486)
	_ = os.WriteFile(manifestPath, []byte("data"), 0o644)
	if _, err := readAndVerifyGeneration(dir, "wrong-digest"); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("readAndVerifyGeneration want digest mismatch, got %v", err)
	}

	// 4. yaml decode error (line 492)
	invalidYAML := []byte(":::invalid")
	invalidSha := sha256Text(invalidYAML)
	_ = os.WriteFile(manifestPath, invalidYAML, 0o644)
	if _, err := readAndVerifyGeneration(dir, invalidSha); err == nil {
		t.Fatal("readAndVerifyGeneration want yaml decode error")
	}

	// 5. unsupported apiVersion (line 495)
	badVerManifest := []byte("apiVersion: unsupported/v99\n")
	badVerSha := sha256Text(badVerManifest)
	_ = os.WriteFile(manifestPath, badVerManifest, 0o644)
	if _, err := readAndVerifyGeneration(dir, badVerSha); err == nil || !strings.Contains(err.Error(), "unsupported generation apiVersion") {
		t.Fatalf("readAndVerifyGeneration want apiVersion error, got %v", err)
	}

	// 6. policies not a dir (line 499)
	policiesDir := filepath.Join(dir, "policies")
	_ = os.WriteFile(policiesDir, []byte("file-not-dir"), 0o644)
	validAPIManifest := []byte("apiVersion: " + generationAPIVersion + "\n")
	validAPISha := sha256Text(validAPIManifest)
	_ = os.WriteFile(manifestPath, validAPIManifest, 0o644)
	if _, err := readAndVerifyGeneration(dir, validAPISha); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("readAndVerifyGeneration want policies dir error, got %v", err)
	}
	_ = os.Remove(policiesDir)
	_ = os.Mkdir(policiesDir, 0o755)

	// 7. unsafe policy reference (line 503)
	unsafeManifest := generationManifest{
		APIVersion: generationAPIVersion,
		Policies:   []generationPolicy{{ID: "bad/id", File: "policies/bad/id.yaml"}},
	}
	unsafeBytes, _ := yaml.Marshal(unsafeManifest)
	unsafeSha := sha256Text(unsafeBytes)
	_ = os.WriteFile(manifestPath, unsafeBytes, 0o644)
	if _, err := readAndVerifyGeneration(dir, unsafeSha); err == nil || !strings.Contains(err.Error(), "unsafe generation policy reference") {
		t.Fatalf("readAndVerifyGeneration want unsafe policy error, got %v", err)
	}

	// 8. policy not regular file (line 508)
	policyPath := filepath.Join(policiesDir, "p1.yaml")
	_ = os.Mkdir(policyPath, 0o755)
	p1Manifest := generationManifest{
		APIVersion: generationAPIVersion,
		Policies:   []generationPolicy{{ID: "p1", File: "policies/p1.yaml", Digest: "dig1"}},
	}
	p1Bytes, _ := yaml.Marshal(p1Manifest)
	p1Sha := sha256Text(p1Bytes)
	_ = os.WriteFile(manifestPath, p1Bytes, 0o644)
	if _, err := readAndVerifyGeneration(dir, p1Sha); err == nil || !strings.Contains(err.Error(), "regular non-symlink file") {
		t.Fatalf("readAndVerifyGeneration want policy regular file error, got %v", err)
	}
	_ = os.Remove(policyPath)

	// 9. policy read error (line 512)
	_ = os.WriteFile(policyPath, []byte("data"), 0o000)
	defer func() { _ = os.Chmod(policyPath, 0o644) }()
	if _, err := readAndVerifyGeneration(dir, p1Sha); err == nil {
		t.Fatal("readAndVerifyGeneration want policy read error")
	}
	_ = os.Chmod(policyPath, 0o644)

	// 10. policy DTQL parse error (line 516)
	_ = os.WriteFile(policyPath, []byte("not-policy"), 0o644)
	if _, err := readAndVerifyGeneration(dir, p1Sha); err == nil {
		t.Fatal("readAndVerifyGeneration want DTQL parse error")
	}

	// 11. policy digest mismatch (line 520)
	doc, err := access.ParseDTQLPolicy(sampleDTQLPolicy("p1", "db1"))
	if err != nil {
		t.Fatal(err)
	}
	docYAML, _ := access.MarshalDTQLPolicyYAML(doc)
	_ = os.WriteFile(policyPath, docYAML, 0o644)
	if _, err := readAndVerifyGeneration(dir, p1Sha); err == nil || !strings.Contains(err.Error(), "digest or id mismatch") {
		t.Fatalf("readAndVerifyGeneration want digest mismatch, got %v", err)
	}
}

func TestAccessGeneration_RevisionAndCleanTree_Gaps(t *testing.T) {
	// 1. committedGenerationRevisionAt: ref == "" returns ("", nil) (line 532)
	rev, err := committedGenerationRevisionAt(context.Background(), "dir", "")
	if err != nil || rev != "" {
		t.Fatalf("committedGenerationRevisionAt empty ref: %v, %v", rev, err)
	}

	dir := t.TempDir()
	origGitCmd := gitCmdRun
	defer func() { gitCmdRun = origGitCmd }()

	// 2. requireCleanAccessTree: git ls-tree error (line 548)
	if err := requireCleanAccessTree(context.Background(), dir); err == nil {
		t.Fatal("requireCleanAccessTree want git error in empty dir")
	}

	runGit := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %s", args, out)
		}
	}
	runGit("init")
	runGit("config", "user.email", "test@test.com")
	runGit("config", "user.name", "Test")

	accessDir := filepath.Join(dir, accessConfigDir)
	_ = os.MkdirAll(accessDir, 0o755)
	manifestPath := filepath.Join(accessDir, accessManifestName)
	_ = os.WriteFile(manifestPath, []byte("enabled: true\n"), 0o644)
	runGit("add", ".")
	runGit("commit", "-m", "init")

	// 3. committedGenerationRevisionAt with invalid yaml (line 540)
	_ = os.WriteFile(manifestPath, []byte(":::invalid"), 0o644)
	runGit("add", ".")
	runGit("commit", "-m", "bad yaml")
	rev, err = committedGenerationRevisionAt(context.Background(), dir, "HEAD")
	if err != nil || rev != "" {
		t.Fatalf("committedGenerationRevisionAt bad yaml want empty, got %v, %v", rev, err)
	}

	// 4. working differs from HEAD (line 559)
	_ = os.WriteFile(manifestPath, []byte("enabled: false\n"), 0o644)
	if err := requireCleanAccessTree(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "differs from HEAD") {
		t.Fatalf("requireCleanAccessTree want differs error, got %v", err)
	}
	_ = os.WriteFile(manifestPath, []byte(":::invalid"), 0o644)

	// 5. gitBlob error (line 555)
	origBlob := gitShowBlob
	defer func() { gitShowBlob = origBlob }()
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		return nil, errors.New("gitBlob fail")
	}
	if err := requireCleanAccessTree(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "gitBlob fail") {
		t.Fatalf("requireCleanAccessTree want gitBlob fail, got %v", err)
	}
	gitShowBlob = origBlob

	// 6. Entries with .generation- prefix (line 571)
	genDir := filepath.Join(accessDir, "generations")
	_ = os.MkdirAll(genDir, 0o755)
	_ = os.MkdirAll(filepath.Join(genDir, ".generation-temp"), 0o755)
	_ = os.WriteFile(filepath.Join(genDir, ".generation-temp", "dummy"), []byte("data"), 0o644)

	// 7. Entries with !isSHA256 (line 574)
	_ = os.MkdirAll(filepath.Join(genDir, "not-a-sha"), 0o755)

	// 8. Symlink in access tree (line 587)
	symChild := filepath.Join(accessDir, "symlink_file")
	_ = os.Symlink(manifestPath, symChild)
	if err := requireCleanAccessTree(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "contains symlink") {
		t.Fatalf("requireCleanAccessTree want symlink error, got %v", err)
	}
	_ = os.Remove(symChild)

	// 9. filepath.Rel error (line 591)
	origRel := filepathRel
	defer func() { filepathRel = origRel }()
	filepathRel = func(basepath, targpath string) (string, error) {
		return "", errors.New("filepathRel fail")
	}
	if err := requireCleanAccessTree(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "filepathRel fail") {
		t.Fatalf("requireCleanAccessTree want filepathRel fail, got %v", err)
	}
	filepathRel = origRel

	// 9b. filepathWalkDir error (line 581)
	origWalk := filepathWalkDir
	defer func() { filepathWalkDir = origWalk }()
	filepathWalkDir = func(root string, fn fs.WalkDirFunc) error {
		return fn(root, nil, errors.New("walk error"))
	}
	if err := requireCleanAccessTree(context.Background(), dir); err == nil || !strings.Contains(err.Error(), "walk error") {
		t.Fatalf("requireCleanAccessTree want walk error, got %v", err)
	}
	filepathWalkDir = origWalk

	// 10. Clean tree success
	_ = os.RemoveAll(filepath.Join(genDir, "not-a-sha"))
	if err := requireCleanAccessTree(context.Background(), dir); err != nil {
		t.Fatalf("requireCleanAccessTree want success, got %v", err)
	}
}

func TestAccessGeneration_AtomicWrite_SyncDir_GitCommitPolicyCAS(t *testing.T) {
	dir := t.TempDir()

	// atomicWriteFile:
	// 1. MkdirAll error (line 619)
	badParent := filepath.Join(dir, "bad_file")
	_ = os.WriteFile(badParent, []byte("data"), 0o644)
	if err := atomicWriteFile(filepath.Join(badParent, "sub", "file"), []byte("x"), 0o644); err == nil {
		t.Fatal("atomicWriteFile want MkdirAll error")
	}

	// 2. os.CreateTemp error (line 623)
	roDir := filepath.Join(dir, "ro_atomic")
	_ = os.MkdirAll(roDir, 0o755)
	origCreateTemp := osCreateTemp
	osCreateTemp = func(string, string) (*os.File, error) { return nil, errors.New("injected CreateTemp failure") }
	if err := atomicWriteFile(filepath.Join(roDir, "file"), []byte("x"), 0o644); err == nil {
		t.Fatal("atomicWriteFile want CreateTemp error")
	}
	osCreateTemp = origCreateTemp

	// 2b. atomicWriteFile write/sync error (line 638)
	defer func() { osCreateTemp = origCreateTemp }()
	osCreateTemp = func(d, pattern string) (*os.File, error) {
		f, err := os.CreateTemp(d, pattern)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
		return f, nil
	}
	if err := atomicWriteFile(filepath.Join(dir, "atom_fail.txt"), []byte("data"), 0o644); err == nil {
		t.Fatal("atomicWriteFile want write error on closed file")
	}
	osCreateTemp = origCreateTemp

	// 3. osRename error (line 641)
	origRename := osRename
	defer func() { osRename = origRename }()
	osRename = func(oldpath, newpath string) error {
		return errors.New("osRename fail")
	}
	if err := atomicWriteFile(filepath.Join(dir, "atom.txt"), []byte("x"), 0o644); err == nil || !strings.Contains(err.Error(), "osRename fail") {
		t.Fatalf("atomicWriteFile want rename fail, got %v", err)
	}
	osRename = origRename

	// syncDir:
	// 1. runtimeGOOS == "windows" returns nil (line 652)
	origGOOS := runtimeGOOS
	defer func() { runtimeGOOS = origGOOS }()
	runtimeGOOS = "windows"
	if err := syncDir("anything"); err != nil {
		t.Fatalf("syncDir windows want nil, got %v", err)
	}
	runtimeGOOS = origGOOS

	// 2. os.Open error on non-existent (line 656)
	if err := syncDir(filepath.Join(dir, "nonexistent")); err == nil {
		t.Fatal("syncDir want open error")
	}

	// gitCommitPolicyCAS:
	// 1. osCreateTemp error (line 664)
	origCreateTemp = osCreateTemp
	defer func() { osCreateTemp = origCreateTemp }()
	osCreateTemp = func(dir, pattern string) (*os.File, error) {
		return nil, errors.New("create temp error")
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", "head1"); err == nil || !strings.Contains(err.Error(), "create temp error") {
		t.Fatalf("gitCommitPolicyCAS want create temp error, got %v", err)
	}
	osCreateTemp = origCreateTemp

	origCmdRun := gitCmdRun
	origStdin := gitCmdRunStdin
	defer func() {
		gitCmdRun = origCmdRun
		gitCmdRunStdin = origStdin
	}()

	// 2. read-tree expectedHead error (line 675)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "read-tree" {
			return []byte("read tree fail"), errors.New("fail")
		}
		return nil, nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", "head1"); err == nil || !strings.Contains(err.Error(), "read tree") {
		t.Fatalf("gitCommitPolicyCAS want read tree error, got %v", err)
	}

	// 3. read-tree --empty error when expectedHead == "" (lines 678, 679)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 1 && args[1] == "--empty" {
			return nil, errors.New("empty fail")
		}
		return nil, nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "empty fail") {
		t.Fatalf("gitCommitPolicyCAS want empty fail, got %v", err)
	}

	// 4. git add error (line 685)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "add" {
			return []byte("add fail"), errors.New("fail")
		}
		return nil, nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "stage policy generation") {
		t.Fatalf("gitCommitPolicyCAS want stage policy error, got %v", err)
	}

	// 5. git hash-object error (line 689)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		return nil, nil
	}
	gitCmdRunStdin = func(ctx context.Context, d string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "hash-object" {
			return []byte("hash fail"), errors.New("fail")
		}
		return []byte("dummy\n"), nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "store active manifest blob") {
		t.Fatalf("gitCommitPolicyCAS want store blob error, got %v", err)
	}

	// 6. update-index error (line 693)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "update-index" {
			return []byte("update-index fail"), errors.New("fail")
		}
		return nil, nil
	}
	gitCmdRunStdin = func(ctx context.Context, d string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		return []byte("blob-sha\n"), nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "stage active manifest") {
		t.Fatalf("gitCommitPolicyCAS want stage active manifest error, got %v", err)
	}

	// 7. write-tree error (line 697)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "write-tree" {
			return nil, errors.New("write tree fail")
		}
		return nil, nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "write tree fail") {
		t.Fatalf("gitCommitPolicyCAS want write tree error, got %v", err)
	}

	// 8. commit-tree error (line 705)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "write-tree" {
			return []byte("tree-sha\n"), nil
		}
		return nil, nil
	}
	gitCmdRunStdin = func(ctx context.Context, d string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "commit-tree" {
			return []byte("commit fail"), errors.New("fail")
		}
		return []byte("blob-sha\n"), nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "create policy commit") {
		t.Fatalf("gitCommitPolicyCAS want create commit error, got %v", err)
	}

	// 9. update-ref error (line 716) and expectedHead == "" (line 713)
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "write-tree" {
			return []byte("tree-sha\n"), nil
		}
		if len(args) > 0 && args[0] == "update-ref" {
			return []byte("update-ref fail"), errors.New("fail")
		}
		return nil, nil
	}
	gitCmdRunStdin = func(ctx context.Context, d string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		return []byte("commit-sha\n"), nil
	}
	if _, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", ""); err == nil || !strings.Contains(err.Error(), "publish policy commit") {
		t.Fatalf("gitCommitPolicyCAS want publish commit error, got %v", err)
	}

	// 10. Success with expectedHead != ""
	gitCmdRun = func(ctx context.Context, d string, env []string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "write-tree" {
			return []byte("tree-sha\n"), nil
		}
		return nil, nil
	}
	commitID, err := gitCommitPolicyCAS(context.Background(), dir, nil, nil, "msg", "existing-head")
	if err != nil || commitID != "commit-sha" {
		t.Fatalf("gitCommitPolicyCAS success want commit-sha, got %v, %v", commitID, err)
	}
}
