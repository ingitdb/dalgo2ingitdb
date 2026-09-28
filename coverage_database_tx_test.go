package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

type customTransform struct{ name string }

func (c customTransform) Name() string  { return c.name }
func (c customTransform) Value() any    { return 1 }
func (c customTransform) String() string { return c.name }

type unmappableType struct {
	Ch chan int
}

type mockColReader struct{}

func (mockColReader) ReadDefinition(dbPath string, opts ...ingitdb.ReadOption) (*ingitdb.Definition, error) {
	return &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{}}, nil
}

type emptyCustomUpdate struct{}

func (emptyCustomUpdate) FieldName() string        { return "" }
func (emptyCustomUpdate) FieldPath() update.FieldPath { return nil }
func (emptyCustomUpdate) Value() any               { return 1 }

func TestTxReadwrite_NumericAndUpdates(t *testing.T) {
	// toNumericFloat64 tests
	types := []any{
		int(1), int8(2), int16(3), int32(4), int64(5),
		uint(6), uint8(7), uint16(8), uint32(9), uint64(10),
		float32(11.5), float64(12.5),
	}
	for _, v := range types {
		f, ok := toNumericFloat64(v)
		if !ok || f == 0 {
			t.Fatalf("toNumericFloat64(%T) failed: got %v, %v", v, f, ok)
		}
	}
	if _, ok := toNumericFloat64("not-a-number"); ok {
		t.Fatal("toNumericFloat64 string: want false")
	}

	// applyDelete with non-map intermediate (line 458)
	root := map[string]any{"a": "string-value"}
	if err := applyDelete(root, []string{"a", "b"}); err != nil {
		t.Fatalf("applyDelete non-map intermediate: %v", err)
	}

	// applyIncrement:
	// lines 475-478: creating intermediate maps
	incRoot := map[string]any{}
	if err := applyIncrement(incRoot, []string{"x", "y", "z"}, 5); err != nil {
		t.Fatalf("applyIncrement create intermediates: %v", err)
	}
	if xMap, ok := incRoot["x"].(map[string]any); !ok {
		t.Fatalf("expected x map, got %T", incRoot["x"])
	} else if yMap, ok := xMap["y"].(map[string]any); !ok {
		t.Fatalf("expected y map, got %T", xMap["y"])
	} else if yMap["z"] != int64(5) {
		t.Fatalf("expected z=5, got %v", yMap["z"])
	}

	// line 482: intermediate not a map
	badIncRoot := map[string]any{"x": 123}
	if err := applyIncrement(badIncRoot, []string{"x", "y"}, 1); err == nil || !strings.Contains(err.Error(), "not a map") {
		t.Fatalf("applyIncrement non-map intermediate: want error, got %v", err)
	}

	// line 493: delta not numeric
	if err := applyIncrement(incRoot, []string{"x", "y", "z"}, "not-numeric"); err == nil || !strings.Contains(err.Error(), "not numeric") {
		t.Fatalf("applyIncrement non-numeric delta: want error, got %v", err)
	}

	// line 500: target field has non-numeric value
	strRoot := map[string]any{"val": "hello"}
	if err := applyIncrement(strRoot, []string{"val"}, 1); err == nil || !strings.Contains(err.Error(), "non-numeric value") {
		t.Fatalf("applyIncrement non-numeric target: want error, got %v", err)
	}

	// line 514: float addition resulting in float assignment
	floatRoot := map[string]any{"val": 1.5}
	if err := applyIncrement(floatRoot, []string{"val"}, 0.25); err != nil {
		t.Fatalf("applyIncrement float: %v", err)
	}
	if floatRoot["val"] != 1.75 {
		t.Fatalf("applyIncrement float: want 1.75, got %v", floatRoot["val"])
	}

	// applyUpdates:
	// line 395: empty FieldName and empty FieldPath
	upRoot := map[string]any{"k": "v"}
	if err := applyUpdates(upRoot, []update.Update{emptyCustomUpdate{}}); err != nil {
		t.Fatalf("applyUpdates empty: %v", err)
	}

	// line 421: unsupported transform
	origIsTransform := dalIsTransform
	defer func() { dalIsTransform = origIsTransform }()
	dalIsTransform = func(v any) (dal.Transform, bool) {
		return customTransform{name: "unsupported_transform"}, true
	}
	customU := update.ByFieldPath(update.FieldPath{"field"}, 1)
	if err := applyUpdates(upRoot, []update.Update{customU}); err == nil || !strings.Contains(err.Error(), "unsupported transform") {
		t.Fatalf("applyUpdates unsupported transform: want error, got %v", err)
	}
	dalIsTransform = origIsTransform

	// ServerTimestamp update
	stU := update.ByFieldPath(update.FieldPath{"ts"}, update.ServerTimestamp)
	if err := applyUpdates(upRoot, []update.Update{stU}); err != nil {
		t.Fatalf("applyUpdates server timestamp: %v", err)
	}
}

func TestTxReadwrite_SnapshotAndErrors(t *testing.T) {
	origStat := osStat
	origReadFile := osReadFile
	defer func() {
		osStat = origStat
		osReadFile = origReadFile
	}()

	r := readwriteTx{snapshots: make(map[string]fileSnapshot)}

	// line 73: snapshot stat error
	osStat = func(name string) (os.FileInfo, error) {
		return nil, errors.New("stat error")
	}
	err := r.snapshot("/path/to/file")
	if err == nil || !strings.Contains(err.Error(), "snapshot stat") {
		t.Fatalf("snapshot: want stat error, got %v", err)
	}

	// line 77: snapshot read error
	tmp := filepath.Join(t.TempDir(), "test.txt")
	_ = os.WriteFile(tmp, []byte("data"), 0o644)
	osStat = origStat
	osReadFile = func(name string) ([]byte, error) {
		return nil, errors.New("read error")
	}
	err = r.snapshot(tmp)
	if err == nil || !strings.Contains(err.Error(), "snapshot read") {
		t.Fatalf("snapshot: want read error, got %v", err)
	}
	osReadFile = origReadFile

	// line 106 & 168: DataToMap error in Set and Insert
	colDef := &ingitdb.CollectionDef{
		ID:         "c",
		RecordFile: &ingitdb.RecordFileDef{RecordType: ingitdb.SingleRecord, Format: ingitdb.RecordFormatYAML, Name: "{key}.yaml"},
	}
	def := &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"c": colDef}}
	tx := readwriteTx{
		readonlyTx: readonlyTx{def: def},
		snapshots:  make(map[string]fileSnapshot),
	}
	badRec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "k"), unmappableType{Ch: make(chan int)})
	if err := tx.Set(context.Background(), badRec); err == nil {
		t.Fatal("Set: want DataToMap error")
	}
	if err := tx.Insert(context.Background(), badRec); err == nil {
		t.Fatal("Insert: want DataToMap error")
	}

	// line 120 & 182: snapshot error in Set and Insert
	osStat = func(name string) (os.FileInfo, error) {
		return nil, errors.New("stat failure")
	}
	goodRec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "k"), map[string]any{"a": 1})
	if err := tx.Set(context.Background(), goodRec); err == nil || !strings.Contains(err.Error(), "snapshot stat") {
		t.Fatalf("Set: want snapshot error, got %v", err)
	}
	if err := tx.Insert(context.Background(), goodRec); err == nil || !strings.Contains(err.Error(), "snapshot stat") {
		t.Fatalf("Insert: want snapshot error, got %v", err)
	}
	osStat = origStat

	// line 347, 355, 359: UpdateRecord errors
	if err := tx.UpdateRecord(context.Background(), badRec, nil); err == nil {
		t.Fatal("UpdateRecord: want DataToMap error")
	}
	badKeyRec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("unknown_col", "k"), map[string]any{"a": 1})
	if err := tx.UpdateRecord(context.Background(), badKeyRec, nil); err == nil {
		t.Fatal("UpdateRecord: want resolveCollection error")
	}

	// UpdateRecord foreign key error (line 359)
	colWithFK := &ingitdb.CollectionDef{
		ID:         "cfk",
		RecordFile: &ingitdb.RecordFileDef{RecordType: ingitdb.SingleRecord, Format: ingitdb.RecordFormatYAML, Name: "{key}.yaml"},
		Columns:    map[string]*ingitdb.ColumnDef{"fk": {Type: ingitdb.ColumnTypeString, ForeignKey: "missing"}},
	}
	def.Collections["cfk"] = colWithFK
	recFK := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("cfk", "k"), map[string]any{"fk": "val"})
	if err := tx.UpdateRecord(context.Background(), recFK, nil); err == nil {
		t.Fatal("UpdateRecord: want foreign key error")
	}

	// line 320: Update resolveCollection error
	if err := tx.Update(context.Background(), dalrecord.NewKeyWithID("unknown_col", "k"), nil); err == nil {
		t.Fatal("Update: want resolveCollection error")
	}

	// Delete and DeleteMulti:
	// line 237: resolveCollection error (non-errCollectionNotInDefinition)
	if err := tx.Delete(context.Background(), nil); err == nil {
		t.Fatal("Delete: want nil key error")
	}
	if err := tx.DeleteMulti(context.Background(), []*dalrecord.Key{nil}); err == nil {
		t.Fatal("DeleteMulti: want error")
	}

	// line 254: snapshot error in Delete
	osStat = func(name string) (os.FileInfo, error) {
		return nil, errors.New("stat failure in delete")
	}
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("c", "k")); err == nil {
		t.Fatal("Delete: want snapshot error")
	}
	osStat = origStat

	// line 265: statErr in Delete
	colDir := t.TempDir()
	colDef.DirPath = colDir
	delPath := resolveRecordPath(colDef, "k")
	tx.snapshots[delPath] = fileSnapshot{exists: true}
	osStat = func(name string) (os.FileInfo, error) {
		if strings.HasSuffix(name, "k.yaml") {
			return nil, errors.New("permission denied")
		}
		return origStat(name)
	}
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("c", "k")); err == nil || !strings.Contains(err.Error(), "dalgo2ingitdb: stat") {
		t.Fatalf("Delete: want statErr, got %v", err)
	}
	osStat = origStat
	delete(tx.snapshots, delPath)

	// line 268: deleteSingleRecordFile error in Delete
	origRemove := osRemove
	defer func() { osRemove = origRemove }()
	osRemove = func(name string) error {
		return errors.New("remove failure")
	}
	targetPath := resolveRecordPath(colDef, "k")
	_ = os.MkdirAll(filepath.Dir(targetPath), 0o755)
	_ = os.WriteFile(targetPath, []byte("a: 1\n"), 0o644)
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("c", "k")); err == nil || !strings.Contains(err.Error(), "remove failure") {
		t.Fatalf("Delete: want deleteSingleRecordFile error, got %v", err)
	}
	osRemove = origRemove

	// line 287: Delete unsupported RecordType
	unsupportedCol := &ingitdb.CollectionDef{
		ID:         "unsupported",
		RecordFile: &ingitdb.RecordFileDef{RecordType: "unknown_type", Format: ingitdb.RecordFormatYAML, Name: "{key}.yaml"},
	}
	def.Collections["unsupported"] = unsupportedCol
	origFKExists := foreignKeyTargetExistsSeam
	defer func() { foreignKeyTargetExistsSeam = origFKExists }()
	foreignKeyTargetExistsSeam = func(col *ingitdb.CollectionDef, key string) (bool, error) {
		return false, nil
	}
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("unsupported", "k")); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("Delete unsupported record type: want error, got %v", err)
	}

	// line 275: readMapOfRecordsFile error in Delete (corrupt file)
	mapDir := t.TempDir()
	mapFilePath := filepath.Join(mapDir, "map.json")
	_ = os.WriteFile(mapFilePath, []byte("invalid-json"), 0o644)
	mapCol := &ingitdb.CollectionDef{
		ID:      "map_col",
		DirPath: mapDir,
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.MapOfRecords,
			Format:     ingitdb.RecordFormatJSON,
			Name:       "map.json",
		},
	}
	def.Collections["map_col"] = mapCol
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("map_col", "k")); err == nil {
		t.Fatal("Delete corrupt map file: want error")
	}

	// line 282: writeMapOfRecordsFile error in Delete
	_ = os.WriteFile(mapFilePath, []byte(`{"k": {"a": 1}}`), 0o644)
	origWriteFile := osWriteFile
	defer func() { osWriteFile = origWriteFile }()
	osWriteFile = func(name string, data []byte, perm os.FileMode) error {
		return errors.New("write map failure")
	}
	if err := tx.Delete(context.Background(), dalrecord.NewKeyWithID("map_col", "k")); err == nil || !strings.Contains(err.Error(), "write map failure") {
		t.Fatalf("Delete: want writeMapOfRecordsFile error, got %v", err)
	}
	osWriteFile = origWriteFile
	foreignKeyTargetExistsSeam = origFKExists

	// line 320: Update resolveCollection error
	upCol := &ingitdb.CollectionDef{
		ID:      "upcol",
		DirPath: t.TempDir(),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
	}
	def.Collections["upcol"] = upCol
	upFilePath := resolveRecordPath(upCol, "k")
	_ = os.MkdirAll(filepath.Dir(upFilePath), 0o755)
	_ = os.WriteFile(upFilePath, []byte("x: 1\n"), 0o644)
	origIsTransform := dalIsTransform
	defer func() { dalIsTransform = origIsTransform }()
	dalIsTransform = func(v any) (dal.Transform, bool) {
		upCol.RecordFile = nil // mutate after Get so resolveCollection fails
		return dal.Increment(1), true
	}
	if err := tx.Update(context.Background(), dalrecord.NewKeyWithID("upcol", "k"), []update.Update{update.ByFieldName("x", 1)}); err == nil || !strings.Contains(err.Error(), "has no record_file definition") {
		t.Fatalf("Update: want resolveCollection error, got %v", err)
	}
	dalIsTransform = origIsTransform
}

func TestDatabase_SecuredAndLifecycleGaps(t *testing.T) {
	dir := t.TempDir()

	// line 115: recoverCommittedGeneration error
	origRecover := recoverCommittedGenerationSeam
	defer func() { recoverCommittedGenerationSeam = origRecover }()
	recoverCommittedGenerationSeam = func(ctx context.Context, root string) error {
		return errors.New("recover committed failure")
	}
	_, err := NewDatabase(dir, nil)
	if err == nil || !strings.Contains(err.Error(), "recover committed failure") {
		t.Fatalf("NewDatabase: want recover error, got %v", err)
	}
	recoverCommittedGenerationSeam = origRecover

	// line 120: nil option in NewDatabase
	_, err = NewDatabase(dir, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "nil database option at index 0") {
		t.Fatalf("NewDatabase nil option: want error, got %v", err)
	}

	// Enable access manifest in dir
	accessDir := filepath.Join(dir, accessConfigDir)
	_ = os.MkdirAll(accessDir, 0o755)
	manifestPath := filepath.Join(accessDir, accessManifestName)
	policyDir := filepath.Join(accessDir, "policies")
	_ = os.MkdirAll(policyDir, 0o755)
	policyContent := "apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: test}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /col/*, rules: [{id: r, effect: allow, operations: [get]}]}]\n"
	_ = os.WriteFile(filepath.Join(policyDir, "p.yaml"), []byte(policyContent), 0o644)
	_ = os.WriteFile(manifestPath, []byte("enabled: true\ndatabase: test\npolicies: [policies/p.yaml]\n"), 0o644)

	// line 151: workingGenerationRevision error
	origWorkGen := workingGenerationRevisionSeam
	defer func() { workingGenerationRevisionSeam = origWorkGen }()
	workingGenerationRevisionSeam = func(root string) (string, error) {
		return "", errors.New("work gen failure")
	}
	_, err = NewDatabase(dir, nil)
	if err == nil || !strings.Contains(err.Error(), "work gen failure") {
		t.Fatalf("NewDatabase working gen error: got %v", err)
	}

	// line 155: newOwnerPolicyController error
	origNewController := newOwnerPolicyControllerSeam
	defer func() { newOwnerPolicyControllerSeam = origNewController }()
	workingGenerationRevisionSeam = func(root string) (string, error) {
		return strings.Repeat("a", 64), nil
	}
	newOwnerPolicyControllerSeam = func(p string) (*OwnerPolicyController, error) {
		return nil, errors.New("controller failure")
	}
	_, err = NewDatabase(dir, nil)
	if err == nil || !strings.Contains(err.Error(), "controller failure") {
		t.Fatalf("NewDatabase controller error: got %v", err)
	}
	newOwnerPolicyControllerSeam = origNewController

	// line 172: accessSecureDB error
	origSecureDB := accessSecureDBSeam
	defer func() { accessSecureDBSeam = origSecureDB }()
	workingGenerationRevisionSeam = func(root string) (string, error) { return "", nil }
	accessSecureDBSeam = func(db dal.DB, opts ...access.DBOption) (dal.DB, error) {
		return nil, errors.New("secure db failure")
	}
	_, err = NewDatabase(dir, nil)
	if err == nil || !strings.Contains(err.Error(), "secure database: secure db failure") {
		t.Fatalf("NewDatabase secure db error: got %v", err)
	}
	accessSecureDBSeam = origSecureDB

	// Test securedDatabase methods:
	dbBackend := &Database{projectPath: dir, reader: mockColReader{}}
	ownerState := &atomic.Pointer[OwnerPolicySnapshot]{}
	ownerState.Store(&OwnerPolicySnapshot{
		Revision: "rev1",
		Config:   access.FilePolicyConfig{Policies: []string{"p1"}},
		Policies: nil,
	})
	secDB := &securedDatabase{
		DB:         dal.NewDB(dbBackend),
		schema:     dbBackend,
		backend:    dbBackend,
		ownerState: ownerState,
	}

	// line 196: ReloadOwnerPolicies with controller == nil
	if _, err := secDB.ReloadOwnerPolicies(context.Background()); err == nil {
		t.Fatal("ReloadOwnerPolicies: want error when controller is nil")
	}

	// line 215: PublishOwnerPolicyGeneration with controller == nil
	if _, err := secDB.PublishOwnerPolicyGeneration(context.Background(), OwnerPolicyGeneration{}, "rev1", "msg"); err == nil {
		t.Fatal("PublishOwnerPolicyGeneration: want error when controller is nil")
	}

	// line 247: OwnerPolicySnapshot with canceled ctx
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := secDB.OwnerPolicySnapshot(canceledCtx); err == nil {
		t.Fatal("OwnerPolicySnapshot: want canceled context error")
	}

	// line 250: OwnerPolicySnapshot with ownerState == nil
	secNilState := &securedDatabase{DB: dal.NewDB(dbBackend)}
	if _, err := secNilState.OwnerPolicySnapshot(context.Background()); err == nil {
		t.Fatal("OwnerPolicySnapshot: want disabled error when ownerState is nil")
	}

	// line 254: OwnerPolicySnapshot with current == nil
	emptyState := &atomic.Pointer[OwnerPolicySnapshot]{}
	secEmptyState := &securedDatabase{DB: dal.NewDB(dbBackend), ownerState: emptyState}
	if _, err := secEmptyState.OwnerPolicySnapshot(context.Background()); err == nil {
		t.Fatal("OwnerPolicySnapshot: want unavailable error when state is nil")
	}

	// line 267: AccessPolicies error when OwnerPolicySnapshot fails
	if _, err := secEmptyState.AccessPolicies(context.Background()); err == nil {
		t.Fatal("AccessPolicies: want error when snapshot fails")
	}

	// lines 272-290: schema forward methods on securedDatabase
	ref := dal.NewRootCollectionRef("col", "")
	_, _ = secDB.ListCollections(context.Background(), nil)
	_, _ = secDB.DescribeCollection(context.Background(), &ref)
	_, _ = secDB.ListIndexes(context.Background(), &ref)
	_, _ = secDB.ListConstraints(context.Background(), &ref)
	_, _ = secDB.ListReferrers(context.Background(), &ref)

	// line 357 & 367: transactionLockPath error in RunReadwriteTransaction & withTransactionReadLock
	origTxLock := transactionLockPathSeam
	defer func() { transactionLockPathSeam = origTxLock }()
	transactionLockPathSeam = func(ctx context.Context, p string) (string, error) {
		return "", errors.New("tx lock path error")
	}
	if err := dbBackend.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "tx lock path error") {
		t.Fatalf("RunReadwriteTransaction: want lock path error, got %v", err)
	}
	if err := dbBackend.withTransactionReadLock(context.Background(), func() error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "tx lock path error") {
		t.Fatalf("withTransactionReadLock: want lock path error, got %v", err)
	}
	transactionLockPathSeam = origTxLock

	// lines 388, 392, 397: transactionLockPath branches on non-git dir
	nonGitDir := t.TempDir()
	origFpAbs := filepathAbs
	origUserCache := userCacheDir
	defer func() {
		filepathAbs = origFpAbs
		userCacheDir = origUserCache
	}()

	// line 388: filepathAbs error
	filepathAbs = func(path string) (string, error) {
		return "", errors.New("abs failure")
	}
	if _, err := transactionLockPath(context.Background(), nonGitDir); err == nil || !strings.Contains(err.Error(), "resolve project path") {
		t.Fatalf("transactionLockPath: want abs error, got %v", err)
	}
	filepathAbs = origFpAbs

	// line 392: userCacheDir error
	userCacheDir = func() (string, error) {
		return "", errors.New("cache failure")
	}
	if _, err := transactionLockPath(context.Background(), nonGitDir); err == nil || !strings.Contains(err.Error(), "resolve cache") {
		t.Fatalf("transactionLockPath: want userCacheDir error, got %v", err)
	}
	userCacheDir = origUserCache

	// line 413 & 420: runReadwriteTransaction rollback errors
	origRestoreSnapshots := restoreSnapshotsSeam
	defer func() { restoreSnapshotsSeam = origRestoreSnapshots }()
	restoreSnapshotsSeam = func(snapshots map[string]fileSnapshot) error {
		return errors.New("restore failed")
	}
	// line 413: worker fails and rollback fails
	err = dbBackend.runReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return errors.New("worker failed")
	})
	if err == nil || !strings.Contains(err.Error(), "rollback failed: restore failed") {
		t.Fatalf("runReadwriteTransaction: want rollback failed error, got %v", err)
	}
	// line 420: git commit fails and rollback fails
	origGitCmd := gitCmdRun
	defer func() { gitCmdRun = origGitCmd }()
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		return nil, errors.New("git commit error")
	}
	restoreSnapshotsSeam = func(snapshots map[string]fileSnapshot) error {
		return errors.New("restore failed")
	}
	_ = exec.Command("git", "init", dbBackend.projectPath).Run()
	err = dbBackend.runReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		*tx.(readwriteTx).written = append(*tx.(readwriteTx).written, "some/path")
		return nil
	}, dal.TxWithMessage("test commit"))
	if err == nil || !strings.Contains(err.Error(), "commit failed") || !strings.Contains(err.Error(), "rollback failed: restore failed") {
		t.Fatalf("runReadwriteTransaction commit & rollback fail: want error, got %v", err)
	}
	restoreSnapshotsSeam = origRestoreSnapshots
	gitCmdRun = origGitCmd

	// lines 382: transactionLockPath git lock dir mkdir failure
	gitTestDir := t.TempDir()
	_ = exec.Command("git", "init", gitTestDir).Run()
	origMkdirAll := osMkdirAll
	defer func() { osMkdirAll = origMkdirAll }()
	osMkdirAll = func(path string, perm os.FileMode) error {
		if strings.Contains(path, "dalgo2ingitdb") {
			return errors.New("mkdir git lock failure")
		}
		return origMkdirAll(path, perm)
	}
	if _, err := transactionLockPath(context.Background(), gitTestDir); err == nil || !strings.Contains(err.Error(), "create Git transaction lock directory") {
		t.Fatalf("transactionLockPath git mkdir: want error, got %v", err)
	}

	// lines 397: transactionLockPath cache lock dir mkdir failure
	osMkdirAll = func(path string, perm os.FileMode) error {
		if strings.Contains(path, "dalgo2ingitdb") {
			return errors.New("mkdir cache lock failure")
		}
		return origMkdirAll(path, perm)
	}
	if _, err := transactionLockPath(context.Background(), nonGitDir); err == nil || !strings.Contains(err.Error(), "create cached transaction lock directory") {
		t.Fatalf("transactionLockPath cache mkdir: want error, got %v", err)
	}
	osMkdirAll = origMkdirAll

	// line 159: NewDatabase controllerReloadSeam error
	workingGenerationRevisionSeam = func(root string) (string, error) {
		return strings.Repeat("b", 64), nil
	}
	origReload := controllerReloadSeam
	defer func() { controllerReloadSeam = origReload }()
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{}, errors.New("reload error in new db")
	}
	if _, err := NewDatabase(dir, nil); err == nil || !strings.Contains(err.Error(), "reload error in new db") {
		t.Fatalf("NewDatabase reload err: want error, got %v", err)
	}

	// line 165: policy provider snapshot unavailable
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{Revision: "rev1", Policies: nil}, nil
	}
	dbWithEmptyPolicies, err := NewDatabase(dir, nil)
	if err != nil {
		t.Fatalf("NewDatabase with empty policies: %v", err)
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("any", "k"), map[string]any{})
	if err := dbWithEmptyPolicies.Get(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "snapshot unavailable") {
		t.Fatalf("Get with empty policy snapshot: want unavailable error, got %v", err)
	}

	// securedDatabase methods: ReloadOwnerPolicies and PublishOwnerPolicyGeneration
	secDB, ok := dbWithEmptyPolicies.(*securedDatabase)
	if !ok {
		t.Fatalf("expected *securedDatabase, got %T", dbWithEmptyPolicies)
	}

	// line 201: ReloadOwnerPolicies reload error
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{}, errors.New("reload failed in reload")
	}
	if _, err := secDB.ReloadOwnerPolicies(context.Background()); err == nil || !strings.Contains(err.Error(), "reload failed in reload") {
		t.Fatalf("ReloadOwnerPolicies: want reload error, got %v", err)
	}

	// line 205: ReloadOwnerPolicies hook error
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{Revision: "rev2"}, nil
	}
	origPubHook := ownerPolicyPublicationHook
	defer func() { ownerPolicyPublicationHook = origPubHook }()
	ownerPolicyPublicationHook = func(stage string) error {
		if stage == "reload_before_activation" {
			return errors.New("hook reload error")
		}
		return nil
	}
	if _, err := secDB.ReloadOwnerPolicies(context.Background()); err == nil || !strings.Contains(err.Error(), "hook reload error") {
		t.Fatalf("ReloadOwnerPolicies: want hook error, got %v", err)
	}
	ownerPolicyPublicationHook = origPubHook

	// line 225: PublishOwnerPolicyGeneration publish error and reload error
	origPublish := controllerPublishSeam
	defer func() { controllerPublishSeam = origPublish }()
	controllerPublishSeam = func(c *OwnerPolicyController, ctx context.Context, candidate OwnerPolicyGeneration, expectedRevision, message string) (OwnerPolicyPublication, error) {
		return OwnerPolicyPublication{}, errors.New("publish error")
	}
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{}, errors.New("reload error after pub fail")
	}
	if _, err := secDB.PublishOwnerPolicyGeneration(context.Background(), OwnerPolicyGeneration{}, "rev1", "msg"); err == nil || !strings.Contains(err.Error(), "failed closed") {
		t.Fatalf("PublishOwnerPolicyGeneration: want failed closed error, got %v", err)
	}

	// line 233: PublishOwnerPolicyGeneration publish success and reload error
	controllerPublishSeam = func(c *OwnerPolicyController, ctx context.Context, candidate OwnerPolicyGeneration, expectedRevision, message string) (OwnerPolicyPublication, error) {
		return OwnerPolicyPublication{Revision: "rev3"}, nil
	}
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{}, errors.New("reload error after pub success")
	}
	if _, err := secDB.PublishOwnerPolicyGeneration(context.Background(), OwnerPolicyGeneration{}, "rev1", "msg"); err == nil || !strings.Contains(err.Error(), "live activation requires reload") {
		t.Fatalf("PublishOwnerPolicyGeneration: want reload error, got %v", err)
	}

	// line 238: PublishOwnerPolicyGeneration hook error on snapshot_activated
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return OwnerPolicySnapshot{Revision: "rev3"}, nil
	}
	ownerPolicyPublicationHook = func(stage string) error {
		if stage == "snapshot_activated" {
			return errors.New("hook activated error")
		}
		return nil
	}
	if _, err := secDB.PublishOwnerPolicyGeneration(context.Background(), OwnerPolicyGeneration{}, "rev1", "msg"); err == nil || !strings.Contains(err.Error(), "hook activated error") {
		t.Fatalf("PublishOwnerPolicyGeneration: want hook error, got %v", err)
	}
	ownerPolicyPublicationHook = origPubHook
}
