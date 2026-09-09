package dalgo2ingitdb

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

const ownerPolicyLayerID = "ingitdb"

// ProtectedAccessConfigurer is the trusted mount-time capability. The raw
// storage boundary remains private; only the fully secured facade is returned.
type ProtectedAccessConfigurer interface {
	ConfigureProtectedAccess(...access.MandatoryParticipant) (dal.DB, *access.EnforcementCoordinator, error)
}

type protectedDatabase struct {
	dal.DB
	schema  dbschema.SchemaReader
	backend *Database
	writer  dal.WriteSession
}

// protectedFactoryDatabase exists only before trusted mount configuration.
// The configured facade deliberately does not implement the factory, so an
// application cannot reconfigure away an already mandatory upper layer.
type protectedFactoryDatabase struct {
	protectedDatabase
	ddl.SchemaModifier
}

func (db *protectedDatabase) Set(ctx context.Context, r dalrecord.Record) error {
	return db.writer.Set(ctx, r)
}
func (db *protectedDatabase) SetMulti(ctx context.Context, r []dalrecord.Record) error {
	return db.writer.SetMulti(ctx, r)
}
func (db *protectedDatabase) Insert(ctx context.Context, r dalrecord.Record, opts ...dal.InsertOption) error {
	return db.writer.Insert(ctx, r, opts...)
}
func (db *protectedDatabase) InsertMulti(ctx context.Context, r []dalrecord.Record, opts ...dal.InsertOption) error {
	return db.writer.InsertMulti(ctx, r, opts...)
}
func (db *protectedDatabase) Delete(ctx context.Context, key *dalrecord.Key) error {
	return db.writer.Delete(ctx, key)
}
func (db *protectedDatabase) DeleteMulti(ctx context.Context, keys []*dalrecord.Key) error {
	return db.writer.DeleteMulti(ctx, keys)
}
func (db *protectedDatabase) Update(ctx context.Context, key *dalrecord.Key, updates []update.Update, pre ...dal.Precondition) error {
	return db.writer.Update(ctx, key, updates, pre...)
}
func (db *protectedDatabase) UpdateRecord(ctx context.Context, r dalrecord.Record, updates []update.Update, pre ...dal.Precondition) error {
	return db.writer.UpdateRecord(ctx, r, updates, pre...)
}
func (db *protectedDatabase) UpdateMulti(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, pre ...dal.Precondition) error {
	return db.writer.UpdateMulti(ctx, keys, updates, pre...)
}

func (db *protectedDatabase) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	return db.schema.ListCollections(ctx, parent)
}
func (db *protectedDatabase) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	return db.schema.DescribeCollection(ctx, ref)
}
func (db *protectedDatabase) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return db.schema.ListIndexes(ctx, ref)
}
func (db *protectedDatabase) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return db.schema.ListConstraints(ctx, ref)
}
func (db *protectedDatabase) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return db.schema.ListReferrers(ctx, ref)
}

func (db *protectedFactoryDatabase) ConfigureProtectedAccess(upper ...access.MandatoryParticipant) (dal.DB, *access.EnforcementCoordinator, error) {
	return configureProtected(db.backend, db.schema, nil, upper)
}

func (db *securedDatabase) ConfigureProtectedAccess(upper ...access.MandatoryParticipant) (dal.DB, *access.EnforcementCoordinator, error) {
	if db.ownerState == nil {
		return nil, nil, errors.New("dalgo2ingitdb: owner policy snapshot unavailable")
	}
	owner := access.MandatoryParticipant{LayerID: ownerPolicyLayerID, Provider: db.ownerPolicyLease}
	return configureProtected(db.backend, db.schema, &owner, upper)
}

type ownerLease struct {
	policies []access.Policy
	revision string
}

func (l *ownerLease) Policies() []access.Policy { return append([]access.Policy(nil), l.policies...) }
func (l *ownerLease) Revision() string          { return l.revision }
func (*ownerLease) Release()                    {}
func (db *securedDatabase) ownerPolicyLease(ctx context.Context) (access.PolicyLease, error) {
	s, err := db.OwnerPolicySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &ownerLease{policies: s.Policies, revision: s.Revision}, nil
}

func configureProtected(backend *Database, schema dbschema.SchemaReader, owner *access.MandatoryParticipant, upper []access.MandatoryParticipant) (dal.DB, *access.EnforcementCoordinator, error) {
	if backend == nil {
		return nil, nil, errors.New("dalgo2ingitdb: protected backend unavailable")
	}
	participants := append([]access.MandatoryParticipant(nil), upper...)
	if owner != nil {
		participants = append(participants, *owner)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, fmt.Errorf("dalgo2ingitdb: protected revision secret: %w", err)
	}
	coordinator, err := access.NewValidatedEnforcementCoordinator(&protectedStorage{db: backend, secret: secret}, backend.validateProtectedCandidate, participants...)
	if err != nil {
		return nil, nil, err
	}
	provider := func(ctx context.Context) ([]access.Policy, error) {
		var all []access.Policy
		var leases []access.PolicyLease
		defer func() {
			for i := len(leases) - 1; i >= 0; i-- {
				leases[i].Release()
			}
		}()
		for _, participant := range participants {
			if participant.Provider == nil {
				continue
			}
			lease, err := participant.Provider(ctx)
			if err != nil || lease == nil || len(lease.Policies()) == 0 {
				if err == nil {
					err = errors.New("empty policy lease")
				}
				return nil, err
			}
			leases = append(leases, lease)
			all = append(all, lease.Policies()...)
		}
		return all, nil
	}
	backend.storedOnlyReads = true
	secured, err := access.SecureDB(dal.NewDB(backend), access.WithDatabasePolicyProvider(provider), access.WithEnforcementCoordinator(coordinator))
	if err != nil {
		return nil, nil, err
	}
	writer, ok := dal.As[dal.WriteSession](secured)
	if !ok {
		return nil, nil, errors.New("dalgo2ingitdb: secured protected writer unavailable")
	}
	return &protectedDatabase{DB: secured, schema: schema, backend: backend, writer: writer}, coordinator, nil
}

type protectedStorage struct {
	db     *Database
	secret []byte
}
type protectedInspection struct{ evidence []access.ProtectedEvidence }

func (s *protectedInspection) Evidence(ctx context.Context) ([]access.ProtectedEvidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneEvidence(s.evidence), nil
}

type protectedExecution struct {
	protectedInspection
	execute func(context.Context) error
	called  bool
}

func (s *protectedExecution) Execute(ctx context.Context) error {
	if s.called {
		return errors.New("dalgo2ingitdb: protected execution already called")
	}
	s.called = true
	return s.execute(ctx)
}

func (s *protectedStorage) WithinProtectedInspection(ctx context.Context, ops []access.ProtectedOperation, fn func(access.ProtectedInspectionStorage) error) error {
	lock, err := transactionLockPath(ctx, s.db.projectPath)
	if err != nil {
		return err
	}
	return withProtectedLock(ctx, lock, true, func() error {
		def, err := s.db.loadDefinition()
		if err != nil {
			return err
		}
		evidence, _, err := s.prepare(ctx, readonlyTx{db: s.db, def: def}, ops)
		if err != nil {
			return err
		}
		return fn(&protectedInspection{evidence: evidence})
	})
}

func (s *protectedStorage) WithinProtectedExecution(ctx context.Context, ops []access.ProtectedOperation, fn func(access.ProtectedExecutionStorage) error) error {
	lock, err := transactionLockPath(ctx, s.db.projectPath)
	if err != nil {
		return err
	}
	return withProtectedLock(ctx, lock, false, func() error {
		def, err := s.db.loadDefinition()
		if err != nil {
			return err
		}
		ro := readonlyTx{db: s.db, def: def}
		evidence, candidates, err := s.prepare(ctx, ro, ops)
		if err != nil {
			return err
		}
		written := &[]string{}
		snapshots := map[string]fileSnapshot{}
		tx := readwriteTx{readonlyTx: ro, written: written, snapshots: snapshots}
		exec := &protectedExecution{protectedInspection: protectedInspection{evidence: evidence}}
		exec.execute = func(execCtx context.Context) error {
			for i, op := range ops {
				if err := execCtx.Err(); err != nil {
					return err
				}
				switch op.Action() {
				case access.Insert:
					if evidence[i].Exists {
						return access.ErrProtectedRecordExists
					}
					if err := tx.Insert(execCtx, dalrecord.NewRecordWithData(op.Key(), candidates[i])); err != nil {
						return err
					}
				case access.Set, access.Update:
					if op.Action() == access.Update && !evidence[i].Exists {
						return access.ErrProtectedResourceUnavailable
					}
					if err := tx.Set(execCtx, dalrecord.NewRecordWithData(op.Key(), candidates[i])); err != nil {
						return err
					}
				case access.Delete:
					if !evidence[i].Exists {
						return access.ErrProtectedResourceUnavailable
					}
					if err := tx.Delete(execCtx, op.Key()); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := fn(exec); err != nil {
			if rb := restoreSnapshots(snapshots); rb != nil {
				return fmt.Errorf("protected execution failed: %w; rollback failed: %v", err, rb)
			}
			return err
		}
		return nil
	})
}

type contextFileLocker interface {
	TryLockContext(context.Context, time.Duration) (bool, error)
	TryRLockContext(context.Context, time.Duration) (bool, error)
	Unlock() error
}

func withProtectedLock(ctx context.Context, path string, shared bool, fn func() error) error {
	lk := newFileLocker(path)
	contextual, ok := lk.(contextFileLocker)
	if !ok {
		if shared {
			return withSharedLock(path, fn)
		}
		return withExclusiveLock(path, fn)
	}
	var locked bool
	var err error
	if shared {
		locked, err = contextual.TryRLockContext(ctx, 10*time.Millisecond)
	} else {
		locked, err = contextual.TryLockContext(ctx, 10*time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("acquire protected lock on %s: %w", path, err)
	}
	if !locked {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("acquire protected lock on %s", path)
	}
	defer contextual.Unlock()
	return fn()
}

func (s *protectedStorage) prepare(ctx context.Context, ro readonlyTx, ops []access.ProtectedOperation) ([]access.ProtectedEvidence, []map[string]any, error) {
	if err := validateProtectedDefinition(ro.def); err != nil {
		return nil, nil, err
	}
	evidence := make([]access.ProtectedEvidence, len(ops))
	candidates := make([]map[string]any, len(ops))
	cols := make([]*ingitdb.CollectionDef, len(ops))
	keys := make([]string, len(ops))
	for i, op := range ops {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		col, key, err := ro.resolveCollection(op.Key())
		if err != nil {
			return nil, nil, err
		}
		if len(orderedComputedColumns(col)) > 0 {
			return nil, nil, fmt.Errorf("dalgo2ingitdb: computed columns are unsupported under protected profile")
		}
		for _, c := range col.Columns {
			if c.ForeignKey != "" {
				return nil, nil, fmt.Errorf("dalgo2ingitdb: foreign keys are unsupported under protected profile")
			}
		}
		path := resolveRecordPath(col, key)
		cols[i], keys[i] = col, key
		raw, rawErr := os.ReadFile(path)
		if rawErr != nil && !os.IsNotExist(rawErr) {
			return nil, nil, rawErr
		}
		rec := dalrecord.NewRecordWithData(op.Key(), map[string]any{})
		err = ro.Get(ctx, rec)
		exists := err == nil
		if err != nil && !dalrecord.IsNotFound(err) {
			return nil, nil, err
		}
		var pre map[string]any
		if exists {
			pre, err = dalrecord.DataToMap(rec.Data())
			if err != nil {
				return nil, nil, err
			}
		}
		candidate := cloneMap(op.Data())
		if op.Action() == access.Update {
			candidate = cloneMap(pre)
			if !exists {
				// Missing-update admission is decided after policy assessment.
				// Keep the image absent so storage does not fabricate a row or
				// dereference a nil pre-image before visibility is established.
				candidate = nil
			}
			for _, u := range op.Updates() {
				if !u.Delete {
					if _, ok := dal.IsTransform(u.Value); ok || u.Value == update.ServerTimestamp {
						return nil, nil, fmt.Errorf("dalgo2ingitdb: update transforms are unsupported under protected profile")
					}
				}
				value := u.Value
				if u.Delete {
					value = update.DeleteField
				}
				if candidate != nil {
					if err := applyFieldUpdate(candidate, u.Path, value); err != nil {
						return nil, nil, err
					}
				}
			}
		}
		if op.Action() == access.Delete {
			candidate = nil
		}
		revision := s.revision(op.CanonicalTarget(), exists, raw)
		evidence[i] = access.ProtectedEvidence{OperationID: op.ID(), CanonicalTarget: op.CanonicalTarget(), SnapshotToken: revision, Exists: exists, PreImage: cloneMap(pre), CandidateImage: cloneMap(candidate), DataRevision: revision, Complete: true}
		candidates[i] = candidate
	}
	if err := s.assignBatchCandidateRevisions(ops, cols, keys, candidates, evidence); err != nil {
		return nil, nil, err
	}
	return evidence, candidates, nil
}

func (s *protectedStorage) assignBatchCandidateRevisions(ops []access.ProtectedOperation, cols []*ingitdb.CollectionDef, keys []string, candidates []map[string]any, evidence []access.ProtectedEvidence) error {
	byPath := map[string][]int{}
	for i, col := range cols {
		byPath[resolveRecordPath(col, keys[i])] = append(byPath[resolveRecordPath(col, keys[i])], i)
	}
	for path, indexes := range byPath {
		first := indexes[0]
		col := cols[first]
		var raw []byte
		var exists bool
		var err error
		switch col.RecordFile.RecordType {
		case ingitdb.SingleRecord:
			i := first
			if ops[i].Action() != access.Delete && candidates[i] != nil {
				raw, err = ingitdb.EncodeRecordContentForCollection(candidates[i], col)
				exists = true
			}
		case ingitdb.MapOfRecords:
			all, readErr := readMapOfRecordsFile(path, col.RecordFile.Format)
			if readErr != nil {
				return readErr
			}
			if all == nil {
				all = map[string]map[string]any{}
			}
			for _, i := range indexes {
				switch {
				case ops[i].Action() == access.Delete:
					delete(all, keys[i])
				case candidates[i] != nil:
					all[keys[i]] = ingitdb.ApplyLocaleToWrite(candidates[i], col.Columns)
				}
			}
			raw, err = ingitdb.EncodeMapOfRecordsContent(all, col.RecordFile.Format, col.ID, col.ColumnsOrder)
			exists = true
		default:
			return fmt.Errorf("dalgo2ingitdb: protected record type %q unsupported", col.RecordFile.RecordType)
		}
		if err != nil {
			return err
		}
		for _, i := range indexes {
			evidence[i].CandidateRevision = s.revision(ops[i].CanonicalTarget(), exists, raw)
		}
	}
	return nil
}

func validateProtectedDefinition(def *ingitdb.Definition) error {
	var visit func(map[string]*ingitdb.CollectionDef) error
	visit = func(collections map[string]*ingitdb.CollectionDef) error {
		for _, col := range collections {
			if len(orderedComputedColumns(col)) > 0 {
				return fmt.Errorf("dalgo2ingitdb: computed columns are unsupported under protected profile")
			}
			for _, column := range col.Columns {
				if column.ForeignKey != "" {
					return fmt.Errorf("dalgo2ingitdb: foreign keys are unsupported under protected profile")
				}
			}
			if err := visit(col.SubCollections); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(def.Collections)
}

func (db *Database) validateProtectedCandidate(ctx context.Context, op access.ProtectedOperation, candidate map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	def, err := db.loadDefinition()
	if err != nil {
		return err
	}
	ro := readonlyTx{db: db, def: def}
	col, key, err := ro.resolveCollection(op.Key())
	if err != nil {
		return err
	}
	if op.Action() == access.Delete {
		return ValidateDelete(def, op.Key().Collection(), key)
	}
	return ValidateWrite(def, fmt.Sprint(op.Action()), op.Key().Collection(), col, key, candidate)
}

func (s *protectedStorage) revision(target string, exists bool, raw []byte) string {
	h := hmac.New(sha256.New, s.secret)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(target)))
	h.Write(size[:])
	h.Write([]byte(target))
	if exists {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}
func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneEvidence(in []access.ProtectedEvidence) []access.ProtectedEvidence {
	out := make([]access.ProtectedEvidence, len(in))
	for i, v := range in {
		v.PreImage = cloneMap(v.PreImage)
		v.CandidateImage = cloneMap(v.CandidateImage)
		out[i] = v
	}
	return out
}

var _ access.ProtectedStorage = (*protectedStorage)(nil)
var _ ProtectedAccessConfigurer = (*protectedFactoryDatabase)(nil)
var _ ProtectedAccessConfigurer = (*securedDatabase)(nil)
