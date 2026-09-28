package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

type mockProtectedWriter struct {
	dal.WriteSession
}

func (m mockProtectedWriter) Set(ctx context.Context, r dalrecord.Record) error { return nil }
func (m mockProtectedWriter) SetMulti(ctx context.Context, r []dalrecord.Record) error {
	return nil
}
func (m mockProtectedWriter) Insert(ctx context.Context, r dalrecord.Record, opts ...dal.InsertOption) error {
	return nil
}
func (m mockProtectedWriter) InsertMulti(ctx context.Context, r []dalrecord.Record, opts ...dal.InsertOption) error {
	return nil
}
func (m mockProtectedWriter) Delete(ctx context.Context, key *dalrecord.Key) error { return nil }
func (m mockProtectedWriter) DeleteMulti(ctx context.Context, keys []*dalrecord.Key) error {
	return nil
}
func (m mockProtectedWriter) Update(ctx context.Context, key *dalrecord.Key, updates []update.Update, pre ...dal.Precondition) error {
	return nil
}
func (m mockProtectedWriter) UpdateRecord(ctx context.Context, r dalrecord.Record, updates []update.Update, pre ...dal.Precondition) error {
	return nil
}
func (m mockProtectedWriter) UpdateMulti(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, pre ...dal.Precondition) error {
	return nil
}

type mockProtectedSchema struct {
	dbschema.SchemaReader
}

func (m mockProtectedSchema) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	return nil, nil
}
func (m mockProtectedSchema) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	return nil, nil
}
func (m mockProtectedSchema) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return nil, nil
}
func (m mockProtectedSchema) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return nil, nil
}
func (m mockProtectedSchema) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return nil, nil
}

type mockDefReader struct {
	def *ingitdb.Definition
	err error
}

func (m mockDefReader) ReadDefinition(dbPath string, opts ...ingitdb.ReadOption) (*ingitdb.Definition, error) {
	return m.def, m.err
}

func TestProtectedDatabase_ForwardMethods(t *testing.T) {
	db := &protectedDatabase{
		writer: mockProtectedWriter{},
		schema: mockProtectedSchema{},
	}
	ctx := context.Background()
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{})
	key := dalrecord.NewKeyWithID("c", "1")

	_ = db.Set(ctx, rec)
	_ = db.SetMulti(ctx, []dalrecord.Record{rec})
	_ = db.Insert(ctx, rec)
	_ = db.InsertMulti(ctx, []dalrecord.Record{rec})
	_ = db.Delete(ctx, key)
	_ = db.DeleteMulti(ctx, []*dalrecord.Key{key})
	_ = db.Update(ctx, key, nil)
	_ = db.UpdateRecord(ctx, rec, nil)
	_ = db.UpdateMulti(ctx, []*dalrecord.Key{key}, nil)

	_, _ = db.ListCollections(ctx, nil)
	_, _ = db.DescribeCollection(ctx, nil)
	_, _ = db.ListIndexes(ctx, nil)
	_, _ = db.ListConstraints(ctx, nil)
	_, _ = db.ListReferrers(ctx, nil)
}

func TestProtected_ConfigureProtectedAccess_Gaps(t *testing.T) {
	dir := t.TempDir()
	backend := &Database{projectPath: dir}

	// protectedFactoryDatabase.ConfigureProtectedAccess (line 92)
	factoryDB := &protectedFactoryDatabase{
		protectedDatabase: protectedDatabase{backend: backend, schema: mockProtectedSchema{}},
	}
	p := access.MandatoryParticipant{
		LayerID: "upper",
		Provider: func(ctx context.Context) (access.PolicyLease, error) {
			return &ownerLease{policies: []access.Policy{nil}}, nil
		},
	}
	if _, _, err := factoryDB.ConfigureProtectedAccess(p); err != nil {
		t.Fatalf("factoryDB.ConfigureProtectedAccess: %v", err)
	}

	// securedDatabase.ConfigureProtectedAccess: ownerState == nil (line 97)
	secDB := &securedDatabase{
		backend:    backend,
		schema:     mockProtectedSchema{},
		ownerState: nil,
	}
	if _, _, err := secDB.ConfigureProtectedAccess(); err == nil {
		t.Fatal("secDB.ConfigureProtectedAccess want ownerState nil error")
	}

	// ownerLease: Policies, Revision, Release (lines 109-110)
	ol := &ownerLease{policies: nil, revision: "rev123"}
	if rev := ol.Revision(); rev != "rev123" {
		t.Fatalf("want rev123, got %s", rev)
	}
	if p := ol.Policies(); len(p) != 0 {
		t.Fatal("want empty policies")
	}
	ol.Release()

	// securedDatabase.ownerPolicyLease error (line 114)
	secDB2 := &securedDatabase{
		ownerState: &atomic.Pointer[OwnerPolicySnapshot]{},
	}
	// ownerState is nil, so OwnerPolicySnapshot returns error
	if _, err := secDB2.ownerPolicyLease(context.Background()); err == nil {
		t.Fatal("ownerPolicyLease: want error")
	}

	// configureProtected: backend == nil (line 121)
	if _, _, err := configureProtected(nil, mockProtectedSchema{}, nil, nil); err == nil {
		t.Fatal("configureProtected: want backend nil error")
	}

	// configureProtected: provider returning error or empty lease (lines 149-152)
	emptyParticipant := access.MandatoryParticipant{
		LayerID: "empty_layer",
		Provider: func(ctx context.Context) (access.PolicyLease, error) {
			return nil, nil // lease == nil
		},
	}
	secDB3, coord, err := configureProtected(backend, mockProtectedSchema{}, nil, []access.MandatoryParticipant{emptyParticipant})
	if err != nil {
		t.Fatalf("configureProtected with participant: %v", err)
	}
	_ = coord
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k1"), map[string]any{})
	// Querying secDB3 invokes the provider and returns empty lease error
	if err := secDB3.Get(context.Background(), rec); err == nil || (!strings.Contains(err.Error(), "policy") && !strings.Contains(err.Error(), "unavailable")) {
		t.Fatalf("secDB3.Get: want empty policy lease error, got %v", err)
	}
}

func TestProtected_InspectionAndExecution(t *testing.T) {
	// cloneEvidence with []byte value (line 521)
	ev := []access.ProtectedEvidence{
		{
			PreImage:       map[string]any{"raw": []byte("hello")},
			CandidateImage: map[string]any{"arr": []any{[]byte("world")}},
		},
	}
	cloned := cloneEvidence(ev)
	if string(cloned[0].PreImage["raw"].([]byte)) != "hello" {
		t.Fatal("cloneEvidence byte slice mismatch")
	}

	// protectedInspection.Evidence: ctx.Err() (line 179)
	insp := &protectedInspection{evidence: ev}
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := insp.Evidence(ctxCancel); err == nil {
		t.Fatal("Evidence canceled ctx: want error")
	}
	if _, err := insp.Evidence(context.Background()); err != nil {
		t.Fatalf("Evidence valid ctx: %v", err)
	}

	// protectedExecution.Execute: called twice (line 192)
	exec := &protectedExecution{
		execute: func(ctx context.Context) error { return nil },
	}
	if err := exec.Execute(context.Background()); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if err := exec.Execute(context.Background()); err == nil || !strings.Contains(err.Error(), "already called") {
		t.Fatalf("second Execute: want already called error, got %v", err)
	}
}

type mockContextLocker struct {
	tryRLockErr error
	tryLockErr  error
	locked      bool
}

func (m mockContextLocker) Lock() error  { return nil }
func (m mockContextLocker) RLock() error { return nil }
func (m mockContextLocker) TryLockContext(ctx context.Context, d time.Duration) (bool, error) {
	return m.locked, m.tryLockErr
}
func (m mockContextLocker) TryRLockContext(ctx context.Context, d time.Duration) (bool, error) {
	return m.locked, m.tryRLockErr
}
func (m mockContextLocker) Unlock() error { return nil }

type mockNonContextLocker struct{}

func (m mockNonContextLocker) Lock() error   { return nil }
func (m mockNonContextLocker) RLock() error  { return nil }
func (m mockNonContextLocker) Unlock() error { return nil }

func TestProtected_WithProtectedLock_Gaps(t *testing.T) {
	tmpPath := filepath.Join(t.TempDir(), "test.lock")
	origFileLocker := newFileLocker
	defer func() { newFileLocker = origFileLocker }()

	// non-contextFileLocker fallback (lines 286-289)
	newFileLocker = func(path string) fileLocker {
		return mockNonContextLocker{}
	}
	if err := withProtectedLock(context.Background(), tmpPath, true, func() error { return nil }); err != nil {
		t.Fatalf("withProtectedLock shared non-context: %v", err)
	}
	if err := withProtectedLock(context.Background(), tmpPath, false, func() error { return nil }); err != nil {
		t.Fatalf("withProtectedLock exclusive non-context: %v", err)
	}

	// contextFileLocker TryRLockContext error (line 298)
	newFileLocker = func(path string) fileLocker {
		return mockContextLocker{tryRLockErr: errors.New("rlock fail")}
	}
	if err := withProtectedLock(context.Background(), tmpPath, true, func() error { return nil }); err == nil {
		t.Fatal("withProtectedLock want rlock fail error")
	}

	// contextFileLocker not locked with canceled ctx (line 302)
	newFileLocker = func(path string) fileLocker {
		return mockContextLocker{locked: false}
	}
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if err := withProtectedLock(ctxCancel, tmpPath, false, func() error { return nil }); err == nil {
		t.Fatal("withProtectedLock want ctx cancel error")
	}

	// contextFileLocker not locked without canceled ctx (line 305)
	if err := withProtectedLock(context.Background(), tmpPath, false, func() error { return nil }); err == nil {
		t.Fatal("withProtectedLock want not locked error")
	}
}

func TestProtected_WithinProtected_StorageAndExecution(t *testing.T) {
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{
			"a": {Type: ingitdb.ColumnTypeInt},
		},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath: dir,
		reader:      mockDefReader{def: def},
	}
	storage := &protectedStorage{db: db, secret: []byte("secret")}

	// Write an initial record
	recPath := resolveRecordPath(colDef, "k1")
	_ = os.MkdirAll(filepath.Dir(recPath), 0o755)
	_ = os.WriteFile(recPath, []byte("a: 1\n"), 0o644)

	// WithinProtectedInspection lock error (line 201)
	origTxLock := transactionLockPathSeam
	defer func() { transactionLockPathSeam = origTxLock }()
	transactionLockPathSeam = func(ctx context.Context, projectPath string) (string, error) {
		return "", errors.New("lock error")
	}
	opInsert, _ := access.NewProtectedInsert("op1", dalrecord.NewKeyWithID("items", "k1"), map[string]any{"a": 2})
	noopInsp := func(access.ProtectedInspectionStorage) error { return nil }
	noopExec := func(access.ProtectedExecutionStorage) error { return nil }
	if err := storage.WithinProtectedInspection(context.Background(), []access.ProtectedOperation{opInsert}, noopInsp); err == nil {
		t.Fatal("WithinProtectedInspection lock error: want error")
	}
	if err := storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opInsert}, noopExec); err == nil {
		t.Fatal("WithinProtectedExecution lock error: want error")
	}
	transactionLockPathSeam = origTxLock

	// loadDefinition error (lines 206, 224)
	dbBad := &Database{projectPath: dir, reader: nil}
	storageBad := &protectedStorage{db: dbBad, secret: []byte("secret")}
	if err := storageBad.WithinProtectedInspection(context.Background(), []access.ProtectedOperation{opInsert}, noopInsp); err == nil {
		t.Fatal("WithinProtectedInspection loadDef: want error")
	}
	if err := storageBad.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opInsert}, noopExec); err == nil {
		t.Fatal("WithinProtectedExecution loadDef: want error")
	}

	// prepare error (lines 210, 229)
	opBadCol, _ := access.NewProtectedInsert("op2", dalrecord.NewKeyWithID("missing_col", "k1"), map[string]any{"a": 2})
	if err := storage.WithinProtectedInspection(context.Background(), []access.ProtectedOperation{opBadCol}, noopInsp); err == nil {
		t.Fatal("WithinProtectedInspection prepare: want error")
	}
	if err := storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opBadCol}, noopExec); err == nil {
		t.Fatal("WithinProtectedExecution prepare: want error")
	}

	// Execution action tests:
	// 1. Insert existing record error (line 243)
	err := storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opInsert}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if !errors.Is(err, access.ErrProtectedRecordExists) {
		t.Fatalf("Insert existing record: want ErrProtectedRecordExists, got %v", err)
	}

	// 2. Insert execution error (line 246)
	opInsertNew, _ := access.NewProtectedInsert("op3", dalrecord.NewKeyWithID("items", "k_new"), map[string]any{"a": 2})
	origWriteFile := osWriteFile
	defer func() { osWriteFile = origWriteFile }()
	osWriteFile = func(name string, data []byte, perm os.FileMode) error {
		return errors.New("write failure in insert")
	}
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opInsertNew}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if err == nil || !strings.Contains(err.Error(), "write failure in insert") {
		t.Fatalf("Insert failure: want error, got %v", err)
	}
	osWriteFile = origWriteFile

	// 3. Update missing record error (line 250)
	opUpdateMissing, _ := access.NewProtectedUpdate("op4", dalrecord.NewKeyWithID("items", "missing_key"), []update.Update{update.ByFieldName("a", 10)}, "")
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opUpdateMissing}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if !errors.Is(err, access.ErrProtectedResourceUnavailable) {
		t.Fatalf("Update missing: want ErrProtectedResourceUnavailable, got %v", err)
	}

	// 4. Update success & Update tx.Set error (line 253)
	opUpdateExisting, _ := access.NewProtectedUpdate("op5", dalrecord.NewKeyWithID("items", "k1"), []update.Update{update.ByFieldName("a", 10)}, "")
	osWriteFile = func(name string, data []byte, perm os.FileMode) error {
		return errors.New("write failure in update")
	}
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opUpdateExisting}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if err == nil || !strings.Contains(err.Error(), "write failure in update") {
		t.Fatalf("Update failure: want error, got %v", err)
	}
	osWriteFile = origWriteFile

	// 5. Delete missing record error (line 257)
	opDeleteMissing, _ := access.NewProtectedDelete("op6", dalrecord.NewKeyWithID("items", "missing_key"), "")
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opDeleteMissing}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if !errors.Is(err, access.ErrProtectedResourceUnavailable) {
		t.Fatalf("Delete missing: want ErrProtectedResourceUnavailable, got %v", err)
	}

	// 6. Delete tx.Delete error (line 260)
	opDeleteExisting, _ := access.NewProtectedDelete("op7", dalrecord.NewKeyWithID("items", "k1"), "")
	origRemove := osRemove
	defer func() { osRemove = origRemove }()
	osRemove = func(name string) error {
		return errors.New("remove failure in delete")
	}
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opDeleteExisting}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(context.Background())
	})
	if err == nil || !strings.Contains(err.Error(), "remove failure in delete") {
		t.Fatalf("Delete failure: want error, got %v", err)
	}
	osRemove = origRemove

	// 7. fn(exec) error AND rollback error (line 268)
	origRestore := restoreSnapshotsSeam
	defer func() { restoreSnapshotsSeam = origRestore }()
	restoreSnapshotsSeam = func(snapshots map[string]fileSnapshot) error {
		return errors.New("rollback fail")
	}
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opUpdateExisting}, func(exec access.ProtectedExecutionStorage) error {
		return errors.New("fn fail")
	})
	if err == nil || !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("fn and rollback fail: want rollback failed error, got %v", err)
	}
	restoreSnapshotsSeam = origRestore

	// 8. execCtx.Err() (line 238)
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	err = storage.WithinProtectedExecution(context.Background(), []access.ProtectedOperation{opUpdateExisting}, func(exec access.ProtectedExecutionStorage) error {
		return exec.Execute(ctxCancel)
	})
	if err == nil {
		t.Fatal("Execute canceled ctx: want error")
	}
}

func TestProtected_Prepare_BranchesAndErrors(t *testing.T) {
	dir := t.TempDir()

	// validateProtectedDefinition: computed columns (line 447)
	colComputed := &ingitdb.CollectionDef{
		ID:      "comp",
		DirPath: filepath.Join(dir, "comp"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{
			"c": {Type: ingitdb.ColumnTypeInt, Formula: "1 + 1"},
		},
	}
	defComp := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"comp": colComputed},
	}
	if err := validateProtectedDefinition(defComp); err == nil {
		t.Fatal("validateProtectedDefinition computed: want error")
	}

	// validateProtectedDefinition: foreign keys (line 451)
	colFK := &ingitdb.CollectionDef{
		ID:      "fk",
		DirPath: filepath.Join(dir, "fk"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{
			"f": {Type: ingitdb.ColumnTypeString, ForeignKey: "other"},
		},
	}
	defFK := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"fk": colFK},
	}
	if err := validateProtectedDefinition(defFK); err == nil {
		t.Fatal("validateProtectedDefinition foreign key: want error")
	}

	// validateProtectedDefinition: subcollections error (line 455)
	colParent := &ingitdb.CollectionDef{
		ID:      "parent",
		DirPath: filepath.Join(dir, "parent"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		SubCollections: map[string]*ingitdb.CollectionDef{"child": colFK},
	}
	defSub := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"parent": colParent},
	}
	if err := validateProtectedDefinition(defSub); err == nil {
		t.Fatal("validateProtectedDefinition subcollection: want error")
	}

	// prepare: ctx.Err() (line 321)
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"a": {Type: ingitdb.ColumnTypeInt}},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath: dir,
		reader:      mockDefReader{def: def},
	}
	storage := &protectedStorage{db: db, secret: []byte("secret")}
	opInsert, _ := access.NewProtectedInsert("op1", dalrecord.NewKeyWithID("items", "k1"), map[string]any{"a": 2})

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	ro := readonlyTx{db: db, def: def}
	if _, _, err := storage.prepare(ctxCancel, ro, []access.ProtectedOperation{opInsert}); err == nil {
		t.Fatal("prepare canceled ctx: want error")
	}

	// prepare: update transforms unsupported (line 366)
	opUpdateTransform, _ := access.NewProtectedUpdate("op2", dalrecord.NewKeyWithID("items", "k1"), []update.Update{update.ByFieldName("a", dal.Increment(1))}, "")
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opUpdateTransform}); err == nil {
		t.Fatal("prepare update transform: want error")
	}

	// prepare: update ServerTimestamp (line 366)
	opUpdateTimestamp, _ := access.NewProtectedUpdate("op3", dalrecord.NewKeyWithID("items", "k1"), []update.Update{update.ByFieldName("a", update.ServerTimestamp)}, "")
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opUpdateTimestamp}); err == nil {
		t.Fatal("prepare update server timestamp: want error")
	}

	// prepare: update field update error (line 375)
	recPath := resolveRecordPath(colDef, "k1")
	_ = os.MkdirAll(filepath.Dir(recPath), 0o755)
	_ = os.WriteFile(recPath, []byte("a: 1\n"), 0o644)
	opUpdateBadPath, _ := access.NewProtectedUpdate("op4", dalrecord.NewKeyWithID("items", "k1"), []update.Update{update.ByFieldPath([]string{"a", "sub"}, 1)}, "")
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opUpdateBadPath}); err == nil {
		t.Fatal("prepare update bad field path: want error")
	}

	// prepare: MapOfRecords branches and errors (lines 414, 417, 422, 430)
	mapCol := &ingitdb.CollectionDef{
		ID:      "map_items",
		DirPath: filepath.Join(dir, "map_items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.MapOfRecords,
			Format:     ingitdb.RecordFormatJSON,
			Name:       "records.json",
		},
		Columns: map[string]*ingitdb.ColumnDef{"x": {Type: ingitdb.ColumnTypeInt}},
	}
	def.Collections["map_items"] = mapCol
	db.reader = mockDefReader{def: def}

	// readMapOfRecordsFile error in assignBatchCandidateRevisions (line 414)
	mapPath := resolveRecordPath(mapCol, "m1")
	_ = os.MkdirAll(filepath.Dir(mapPath), 0o755)
	_ = os.WriteFile(mapPath, []byte("invalid-json"), 0o644)
	opMap, _ := access.NewProtectedInsert("op_m", dalrecord.NewKeyWithID("map_items", "m1"), map[string]any{"x": 10})
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opMap}); err == nil {
		t.Fatal("prepare map corrupt: want error")
	}

	// all == nil & Delete action in MapOfRecords (lines 417, 422)
	_ = os.Remove(mapPath)
	opMapDelete, _ := access.NewProtectedDelete("op_md", dalrecord.NewKeyWithID("map_items", "m1"), "")
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opMapDelete}); err != nil {
		t.Fatalf("prepare map delete on empty: %v", err)
	}

	// unsupported record type in assignBatchCandidateRevisions (line 430)
	mapCol.RecordFile.RecordType = "unsupported_rec_type"
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opMap}); err == nil {
		t.Fatal("prepare unsupported record type: want error")
	}
}

func TestProtected_ValidateProtectedCandidate_Gaps(t *testing.T) {
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"a": {Type: ingitdb.ColumnTypeInt}},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath: dir,
		reader:      mockDefReader{def: def},
	}

	opDelete, _ := access.NewProtectedDelete("op_del", dalrecord.NewKeyWithID("items", "k1"), "")
	opInsert, _ := access.NewProtectedInsert("op_ins", dalrecord.NewKeyWithID("items", "k1"), map[string]any{"a": 1})

	// ctx.Err() (line 465)
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.validateProtectedCandidate(ctxCancel, opInsert, nil); err == nil {
		t.Fatal("validateProtectedCandidate canceled ctx: want error")
	}

	// loadDefinition error (line 469)
	dbBad := &Database{projectPath: dir, reader: nil}
	if err := dbBad.validateProtectedCandidate(context.Background(), opInsert, nil); err == nil {
		t.Fatal("validateProtectedCandidate loadDef: want error")
	}

	// resolveCollection error (line 474)
	opBadCol, _ := access.NewProtectedInsert("op_bad", dalrecord.NewKeyWithID("missing", "k1"), nil)
	if err := db.validateProtectedCandidate(context.Background(), opBadCol, nil); err == nil {
		t.Fatal("validateProtectedCandidate resolveCollection: want error")
	}

	// Delete action calls ValidateDelete (line 477)
	if err := db.validateProtectedCandidate(context.Background(), opDelete, nil); err != nil {
		t.Fatalf("validateProtectedCandidate Delete: %v", err)
	}
}

type nonWriterDB struct {
	dal.DB
}

func TestProtected_ConfigureProtected_FailureBranches(t *testing.T) {
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"a": {Type: ingitdb.ColumnTypeInt}},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath: dir,
		reader:      mockDefReader{def: def},
	}

	// 1. ownerLease.Release() (line 110)
	l := &ownerLease{}
	l.Release()

	factory := &protectedFactoryDatabase{
		protectedDatabase: protectedDatabase{
			backend: db,
			schema:  db,
		},
	}

	// 2. randReadSeam error (line 129)
	origRand := randReadSeam
	defer func() { randReadSeam = origRand }()
	randReadSeam = func(b []byte) (int, error) { return 0, errors.New("rand fail") }
	if _, _, err := factory.ConfigureProtectedAccess(); err == nil || !strings.Contains(err.Error(), "rand fail") {
		t.Fatalf("ConfigureProtectedAccess rand fail: want error, got %v", err)
	}
	randReadSeam = origRand

	// 3. newValidatedCoordinatorSeam error (line 133)
	origCoord := newValidatedCoordinatorSeam
	defer func() { newValidatedCoordinatorSeam = origCoord }()
	newValidatedCoordinatorSeam = func(storage access.ProtectedStorage, validator access.CandidateValidator, participants ...access.MandatoryParticipant) (*access.EnforcementCoordinator, error) {
		return nil, errors.New("coord fail")
	}
	if _, _, err := factory.ConfigureProtectedAccess(); err == nil || !strings.Contains(err.Error(), "coord fail") {
		t.Fatalf("ConfigureProtectedAccess coord fail: want error, got %v", err)
	}
	newValidatedCoordinatorSeam = origCoord

	participant := access.MandatoryParticipant{
		LayerID: "test_layer",
		Provider: func(context.Context) (access.PolicyLease, error) {
			return &ownerLease{}, nil
		},
	}

	// 4. accessSecureDBSeam error (line 162)
	origSecure := accessSecureDBSeam
	defer func() { accessSecureDBSeam = origSecure }()
	accessSecureDBSeam = func(target dal.DB, options ...access.DBOption) (dal.DB, error) {
		return nil, errors.New("secure fail")
	}
	if _, _, err := factory.ConfigureProtectedAccess(participant); err == nil || !strings.Contains(err.Error(), "secure fail") {
		t.Fatalf("ConfigureProtectedAccess secure fail: want error, got %v", err)
	}
	accessSecureDBSeam = origSecure

	// 5. nonWriterDB error (line 166)
	accessSecureDBSeam = func(target dal.DB, options ...access.DBOption) (dal.DB, error) {
		return nonWriterDB{DB: target}, nil
	}
	if _, _, err := factory.ConfigureProtectedAccess(participant); err == nil || !strings.Contains(err.Error(), "secured protected writer unavailable") {
		t.Fatalf("ConfigureProtectedAccess nonWriterDB: want error, got %v", err)
	}
	accessSecureDBSeam = origSecure
}

func TestProtected_Prepare_MoreGaps(t *testing.T) {
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"a": {Type: ingitdb.ColumnTypeInt}},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath: dir,
		reader:      mockDefReader{def: def},
	}
	storage := &protectedStorage{db: db, secret: []byte("testsecret1234567890123456789012")}
	ro := readonlyTx{db: db, def: def}

	opInsert, _ := access.NewProtectedInsert("op1", dalrecord.NewKeyWithID("items", "k1"), map[string]any{"a": 1})

	// 1. prepare with computed column in def (line 313)
	defComp := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{
			"comp": {
				ID:      "comp",
				DirPath: filepath.Join(dir, "comp"),
				RecordFile: &ingitdb.RecordFileDef{
					RecordType: ingitdb.SingleRecord,
					Format:     ingitdb.RecordFormatYAML,
					Name:       "{key}.yaml",
				},
				Columns: map[string]*ingitdb.ColumnDef{"c": {Type: ingitdb.ColumnTypeInt, Formula: "1+1"}},
			},
		},
	}
	roBadDef := readonlyTx{db: db, def: defComp}
	if _, _, err := storage.prepare(context.Background(), roBadDef, []access.ProtectedOperation{opInsert}); err == nil {
		t.Fatal("prepare bad def: want error")
	}

	// 2. orderedComputedColumns bypass seam (line 328)
	origValDef := validateProtectedDefSeam
	defer func() { validateProtectedDefSeam = origValDef }()
	validateProtectedDefSeam = func(d *ingitdb.Definition) error { return nil }

	colComp := &ingitdb.CollectionDef{
		ID:      "comp",
		DirPath: filepath.Join(dir, "comp"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"c": {Type: ingitdb.ColumnTypeInt, Formula: "1+1"}},
	}
	defBypassComp := &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"comp": colComp}}
	roBypassComp := readonlyTx{db: db, def: defBypassComp}
	opComp, _ := access.NewProtectedInsert("op_c", dalrecord.NewKeyWithID("comp", "k1"), map[string]any{"c": 2})
	if _, _, err := storage.prepare(context.Background(), roBypassComp, []access.ProtectedOperation{opComp}); err == nil || !strings.Contains(err.Error(), "computed columns are unsupported") {
		t.Fatalf("prepare comp bypassed: want error, got %v", err)
	}

	// 3. foreign keys bypass seam (line 332)
	colFK := &ingitdb.CollectionDef{
		ID:      "fk_col",
		DirPath: filepath.Join(dir, "fk_col"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
		Columns: map[string]*ingitdb.ColumnDef{"f": {Type: ingitdb.ColumnTypeString, ForeignKey: "other(id)"}},
	}
	defBypassFK := &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"fk_col": colFK}}
	roBypassFK := readonlyTx{db: db, def: defBypassFK}
	opFK, _ := access.NewProtectedInsert("op_fk", dalrecord.NewKeyWithID("fk_col", "k1"), map[string]any{"f": "v"})
	if _, _, err := storage.prepare(context.Background(), roBypassFK, []access.ProtectedOperation{opFK}); err == nil || !strings.Contains(err.Error(), "foreign keys are unsupported") {
		t.Fatalf("prepare fk bypassed: want error, got %v", err)
	}
	validateProtectedDefSeam = origValDef

	// 4. rawErr non-NotExist in osReadFile (line 339)
	origReadFile := osReadFile
	defer func() { osReadFile = origReadFile }()
	osReadFile = func(name string) ([]byte, error) { return nil, errors.New("read raw file error") }
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opInsert}); err == nil || !strings.Contains(err.Error(), "read raw file error") {
		t.Fatalf("prepare raw read error: want error, got %v", err)
	}
	osReadFile = origReadFile

	// 5. dataToMapSeam error on existing record (line 351)
	itemPath := resolveRecordPath(colDef, "k1")
	_ = os.MkdirAll(filepath.Dir(itemPath), 0o755)
	_ = os.WriteFile(itemPath, []byte("a: 1\n"), 0o644)
	opUpdate, _ := access.NewProtectedUpdate("op_up", dalrecord.NewKeyWithID("items", "k1"), []update.Update{update.ByFieldName("a", 2)}, "")

	origD2M := dataToMapSeam
	defer func() { dataToMapSeam = origD2M }()
	dataToMapSeam = func(data any) (map[string]any, error) { return nil, errors.New("dataToMap fail") }
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opUpdate}); err == nil || !strings.Contains(err.Error(), "dataToMap fail") {
		t.Fatalf("prepare dataToMap fail: want error, got %v", err)
	}
	dataToMapSeam = origD2M

	// 6. encode error in assignBatchCandidateRevisions (lines 388, 433)
	origEncode := encodeRecordContentSeam
	defer func() { encodeRecordContentSeam = origEncode }()
	encodeRecordContentSeam = func(data any, col *ingitdb.CollectionDef) ([]byte, error) {
		return nil, errors.New("encode fail")
	}
	opInsert2, _ := access.NewProtectedInsert("op_ins2", dalrecord.NewKeyWithID("items", "k2"), map[string]any{"a": 1})
	if _, _, err := storage.prepare(context.Background(), ro, []access.ProtectedOperation{opInsert2}); err == nil || !strings.Contains(err.Error(), "encode fail") {
		t.Fatalf("prepare encode error: want error, got %v", err)
	}
	encodeRecordContentSeam = origEncode
}

func TestProtected_AssignBatchCandidateRevisions_Gaps(t *testing.T) {
	origReadMap := readMapOfRecordsFileSeam
	defer func() { readMapOfRecordsFileSeam = origReadMap }()
	readMapOfRecordsFileSeam = func(path string, format ingitdb.RecordFormat) (map[string]map[string]any, error) {
		return nil, errors.New("read map error")
	}

	storage := &protectedStorage{}
	op, _ := access.NewProtectedInsert("op1", dalrecord.NewKeyWithID("map_items", "m1"), map[string]any{"x": 1})
	colMap := &ingitdb.CollectionDef{
		ID:      "map_items",
		DirPath: "map_items",
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.MapOfRecords,
			Format:     ingitdb.RecordFormatJSON,
			Name:       "records.json",
		},
	}
	evidence := make([]access.ProtectedEvidence, 1)
	err := storage.assignBatchCandidateRevisions([]access.ProtectedOperation{op}, []*ingitdb.CollectionDef{colMap}, []string{"m1"}, []map[string]any{{"x": 1}}, evidence)
	if err == nil || !strings.Contains(err.Error(), "read map error") {
		t.Fatalf("assignBatchCandidateRevisions readMap fail: want error, got %v", err)
	}

	colUnsupported := &ingitdb.CollectionDef{
		ID:      "bad_items",
		DirPath: "bad_items",
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: "unsupported",
		},
	}
	err = storage.assignBatchCandidateRevisions([]access.ProtectedOperation{op}, []*ingitdb.CollectionDef{colUnsupported}, []string{"m1"}, []map[string]any{{"x": 1}}, evidence)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("assignBatchCandidateRevisions unsupported: want error, got %v", err)
	}
}

