package dalgo2ingitdb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	"github.com/dal-go/dalgo/recordset"

	dalrecord "github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// Database is the dal.DB implementation for inGitDB projects on the local
// filesystem. It implements the schema-management capability interfaces
// (dbschema.SchemaReader, ddl.SchemaModifier, ddl.TransactionalDDL), the
// dal.DB record-access methods, and reports dal.NoConcurrency — concurrent
// connections are NOT advertised as safe (see the field comment for why).
//
// Record access loads the project Definition once per transaction via the
// injected CollectionsReader; individual file operations take a shared
// (read) or exclusive (write) advisory lock on the affected file.
// ExecuteQueryToRecordsetReader is not yet implemented and returns
// dal.ErrNotSupported.
type Database struct {
	// dal.NoConcurrency makes SupportsConcurrentConnections() report false.
	//
	// An inGitDB database is a git working tree. We do take gofrs/flock
	// advisory locks per file (shared for reads, exclusive for writes) as
	// defence-in-depth, but that is NOT a basis to advertise safe concurrent
	// connections, because:
	//   - flock is ADVISORY on Unix: it only binds processes that also call
	//     flock. A plain `git`, an editor, or `rm` ignores it entirely — and
	//     on Unix can even unlink a file out from under a held lock. It is
	//     mandatory only on Windows (LockFileEx), so the protection is not
	//     cross-platform.
	//   - locks are PER FILE, so a change spanning multiple files (e.g. a
	//     collection's definition.yaml plus root-collections.yaml, or a
	//     subsequent git commit) is not atomic as a unit.
	// The honest cross-platform contract is therefore single-writer: callers
	// MUST NOT open concurrent writing connections against the same tree.
	dal.NoConcurrency

	projectPath string
	reader      ingitdb.CollectionsReader
	// storedOnlyReads keeps derived values out of the adapter's raw read
	// result. The policy wrapper can authorize stored fields, but it cannot yet
	// authorize every dependency used to derive a computed field.
	storedOnlyReads bool
}

type databaseOptions struct {
	storedOnlyReads bool
}

// DatabaseOption configures adapter behavior selected before the database is
// exposed to callers.
type DatabaseOption func(*databaseOptions)

// WithStoredOnlyReads prevents evaluation and return of computed columns. It
// is intended for callers that apply an access-policy wrapper above this
// adapter and cannot authorize every dependency used by a derived value.
func WithStoredOnlyReads() DatabaseOption {
	return func(options *databaseOptions) { options.storedOnlyReads = true }
}

// NewDatabase constructs a Database rooted at projectPath. The reader is
// used to load the project Definition at the start of each transaction
// and inside DB-level record-access methods. Returns an error if
// projectPath is empty or does not exist; the constructor does NOT load
// any collection definitions.
func NewDatabase(projectPath string, reader ingitdb.CollectionsReader, options ...DatabaseOption) (dal.DB, error) {
	if projectPath == "" {
		return nil, errors.New("dalgo2ingitdb: projectPath is required")
	}
	info, err := os.Stat(projectPath)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: stat %s: %w", projectPath, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("dalgo2ingitdb: %s is not a directory", projectPath)
	}
	var settings databaseOptions
	for i, option := range options {
		if option == nil {
			return nil, fmt.Errorf("dalgo2ingitdb: nil database option at index %d", i)
		}
		option(&settings)
	}
	backend := &Database{
		projectPath:     projectPath,
		reader:          reader,
		storedOnlyReads: settings.storedOnlyReads,
	}
	db := dal.NewDB(backend)
	config, present, err := readAccessManifest(projectPath)
	if err != nil {
		return nil, err
	}
	if !present || !config.Enabled {
		return db, nil
	}
	backend.storedOnlyReads = true
	policies, err := access.LoadPolicyFiles(filepath.Join(projectPath, accessConfigDir), config)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: load access policies: %w", err)
	}
	secured, err := access.SecureDB(db, access.WithDatabasePolicies(policies...))
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: secure database: %w", err)
	}
	return &securedDatabase{DB: secured, schema: backend}, nil
}

// securedDatabase preserves read-only schema introspection without making the
// underlying backend reachable through dal.BackendOf. Mutation capabilities
// must not be forwarded because they do not yet have policy semantics.
type securedDatabase struct {
	dal.DB
	schema dbschema.SchemaReader
}

func (db *securedDatabase) ListCollections(ctx context.Context, parent *dalrecord.Key) ([]dal.CollectionRef, error) {
	return db.schema.ListCollections(ctx, parent)
}

func (db *securedDatabase) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	return db.schema.DescribeCollection(ctx, ref)
}

func (db *securedDatabase) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return db.schema.ListIndexes(ctx, ref)
}

func (db *securedDatabase) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return db.schema.ListConstraints(ctx, ref)
}

func (db *securedDatabase) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return db.schema.ListReferrers(ctx, ref)
}

// DatabaseID is the name reported by Database.ID() and used as the
// Adapter name.
const DatabaseID = "dalgo2ingitdb"

// ID returns the driver identifier.
func (db *Database) ID() string { return DatabaseID }

// Adapter returns the dalgo adapter descriptor.
func (db *Database) Adapter() dal.Adapter {
	return dal.NewAdapter(DatabaseID, "v0.0.1")
}

// Schema returns nil — inGitDB does not yet expose a dal.Schema view of
// its collection definitions. Callers needing schema introspection should
// use dbschema.SchemaReader instead.
func (db *Database) Schema() dal.Schema { return nil }

// SupportsTransactionalDDL satisfies ddl.TransactionalDDL by reporting
// that this driver does NOT guarantee all-or-nothing for multi-op
// AlterCollection calls. A failure mid-sequence leaves earlier ops
// applied; the caller receives a *ddl.PartialSuccessError.
func (db *Database) SupportsTransactionalDDL() bool { return false }

// loadDefinition reads the project's Definition via the injected reader.
// Returns an error when no reader has been wired up.
func (db *Database) loadDefinition() (*ingitdb.Definition, error) {
	if db.reader == nil {
		return nil, errors.New("dalgo2ingitdb: no CollectionsReader configured")
	}
	def, err := db.reader.ReadDefinition(db.projectPath)
	if err != nil {
		return nil, fmt.Errorf("dalgo2ingitdb: read definition: %w", err)
	}
	return def, nil
}

// RunReadonlyTransaction loads the project Definition and invokes the
// worker with a readonly transaction. The Definition is captured at the
// start of the transaction; subsequent on-disk schema changes are not
// observed within the transaction.
func (db *Database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return db.withTransactionReadLock(ctx, func() error {
		def, err := db.loadDefinition()
		if err != nil {
			return err
		}
		opts := dal.NewTransactionOptions(options...)
		return f(ctx, readonlyTx{db: db, def: def, opts: opts})
	})
}

// RunReadwriteTransaction loads the project Definition and invokes the
// worker with a read-write transaction. Writes are journaled in-memory before
// their first filesystem mutation. If the worker or the optional Git commit
// fails, the touched files are restored to their exact pre-transaction state.
// This is deliberately a single-writer transaction: it makes a failed
// multi-record Synchestra state transition recoverable without pretending that
// a Git worktree supports concurrent multi-writer transactions.
func (db *Database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	// This lock covers definition load, all record mutations, rollback, and the
	// optional Git ref update. It serialises cooperating Database writers for
	// the complete transaction; Git/editor processes that ignore advisory locks
	// remain outside this adapter's explicit single-writer contract.
	lockPath, err := transactionLockPath(ctx, db.projectPath)
	if err != nil {
		return err
	}
	return withExclusiveLock(lockPath, func() error {
		return db.runReadwriteTransaction(ctx, f, options...)
	})
}

func (db *Database) withTransactionReadLock(ctx context.Context, fn func() error) error {
	lockPath, err := transactionLockPath(ctx, db.projectPath)
	if err != nil {
		return err
	}
	return withSharedLock(lockPath, fn)
}

// transactionLockPath deliberately keeps the transaction lock out of the
// project worktree. Git gives each linked worktree a private git-dir, while a
// non-Git project uses a deterministic cache path keyed by its absolute root.
func transactionLockPath(ctx context.Context, projectPath string) (string, error) {
	if out, err := exec.CommandContext(ctx, "git", "-C", projectPath, "rev-parse", "--git-path", "dalgo2ingitdb/transaction.lock").Output(); err == nil {
		path := filepath.Clean(strings.TrimSpace(string(out)))
		if !filepath.IsAbs(path) {
			path = filepath.Join(projectPath, path)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return "", fmt.Errorf("dalgo2ingitdb: create Git transaction lock directory: %w", err)
		}
		return path, nil
	}
	abs, err := filepath.Abs(projectPath)
	if err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: resolve project path for transaction lock: %w", err)
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: resolve cache transaction lock directory: %w", err)
	}
	hash := sha256.Sum256([]byte(abs))
	path := filepath.Join(cacheDir, "dalgo2ingitdb", "locks", fmt.Sprintf("%x.lock", hash[:]))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("dalgo2ingitdb: create cached transaction lock directory: %w", err)
	}
	return path, nil
}

func (db *Database) runReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	def, err := db.loadDefinition()
	if err != nil {
		return err
	}
	opts := dal.NewTransactionOptions(options...)
	written := &[]string{}
	snapshots := make(map[string]fileSnapshot)
	tx := readwriteTx{readonlyTx: readonlyTx{db: db, def: def, opts: opts}, written: written, snapshots: snapshots}
	if err = f(ctx, tx); err != nil {
		if rollbackErr := restoreSnapshots(snapshots); rollbackErr != nil {
			return fmt.Errorf("transaction failed: %w; rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	if msg := opts.Message(); msg != "" && len(*written) > 0 {
		if err = gitCommitPaths(ctx, db.projectPath, *written, msg); err != nil {
			if rollbackErr := restoreSnapshots(snapshots); rollbackErr != nil {
				return fmt.Errorf("commit failed: %w; rollback failed: %v", err, rollbackErr)
			}
			return err
		}
	}
	return nil
}

// Get loads a single record. See readonlyTx.Get for semantics.
func (db *Database) Get(ctx context.Context, record dalrecord.Record) error {
	return db.withTransactionReadLock(ctx, func() error {
		def, err := db.loadDefinition()
		if err != nil {
			return err
		}
		return readonlyTx{db: db, def: def}.Get(ctx, record)
	})
}

// Exists reports whether the record identified by key exists on disk.
func (db *Database) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	var exists bool
	err := db.withTransactionReadLock(ctx, func() error {
		def, err := db.loadDefinition()
		if err != nil {
			return err
		}
		exists, err = readonlyTx{db: db, def: def}.Exists(ctx, key)
		return err
	})
	return exists, err
}

// GetMulti loads multiple records.
func (db *Database) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	return db.withTransactionReadLock(ctx, func() error {
		def, err := db.loadDefinition()
		if err != nil {
			return err
		}
		return readonlyTx{db: db, def: def}.GetMulti(ctx, records)
	})
}

// ExecuteQueryToRecordsReader runs a structured query against a single
// collection. See readonlyTx.ExecuteQueryToRecordsReader for supported
// query features.
func (db *Database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	var reader dal.RecordsReader
	err := db.withTransactionReadLock(ctx, func() error {
		def, err := db.loadDefinition()
		if err != nil {
			return err
		}
		reader, err = readonlyTx{db: db, def: def}.ExecuteQueryToRecordsReader(ctx, query)
		return err
	})
	return reader, err
}

// ExecuteQueryToRecordsetReader is not implemented yet; callers should
// use ExecuteQueryToRecordsReader instead.
func (db *Database) ExecuteQueryToRecordsetReader(_ context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}

// Compile-time interface checks. SchemaReader / SchemaModifier assertions
// live in schema_reader.go / schema_modifier.go.
var (
	_ dal.Backend          = (*Database)(nil)
	_ ddl.TransactionalDDL = (*Database)(nil)
)
