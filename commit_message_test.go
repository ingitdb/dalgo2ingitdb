package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

// gitInit initialises a git repo at dir with a usable identity so commits work
// deterministically in CI (no global config, no GPG signing).
func gitInit(t *testing.T, dir string) {
	t.Helper()
	for _, args := range [][]string{
		{"init"},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Test"},
		{"config", "commit.gpgsign", "false"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func gitIndexBytes(t *testing.T, dir string) []byte {
	t.Helper()
	indexPath := git(t, dir, "rev-parse", "--git-path", "index")
	if !filepath.IsAbs(indexPath) {
		indexPath = filepath.Join(dir, indexPath)
	}
	contents, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func franceRecord() record.Record {
	return record.NewRecordWithData(
		record.NewKeyWithID("countries", "france"),
		map[string]any{"name": "France", "population": 67000000},
	)
}

// TestRunReadwriteTransaction_CommitsWithMessage verifies that a read-write
// transaction with a message commits exactly the files it wrote, using the
// message as the commit subject, when the project is a git repository.
func TestRunReadwriteTransaction_CommitsWithMessage(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)

	const msg = "add France"
	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}, dal.TxWithMessage(msg)); err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}

	if got := git(t, root, "log", "-1", "--pretty=%s"); got != msg {
		t.Errorf("commit subject: got %q, want %q", got, msg)
	}
	if n := git(t, root, "rev-list", "--count", "HEAD"); n != "1" {
		t.Errorf("commit count: got %s, want 1", n)
	}
	// Only the written record file is committed; schema/registration files the
	// harness created remain untracked.
	files := git(t, root, "show", "--name-only", "--pretty=format:", "HEAD")
	if !strings.Contains(files, "france") {
		t.Errorf("committed files should include the france record, got:\n%s", files)
	}
	if strings.Contains(files, ".ingitdb") {
		t.Errorf("schema/registration files must not be committed, got:\n%s", files)
	}
}

// TestRunReadwriteTransaction_NoMessageNoCommit verifies that without a message
// the transaction writes files but creates no commit (behaviour unchanged).
func TestRunReadwriteTransaction_NoMessageNoCommit(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)
	git(t, root, "commit", "--allow-empty", "-m", "init")

	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}); err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}

	if n := git(t, root, "rev-list", "--count", "HEAD"); n != "1" {
		t.Errorf("no-message tx must not commit: commit count got %s, want 1", n)
	}
	// The record was still written (just left uncommitted in the working tree).
	got := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Fatalf("record should still be written: %v", err)
	}
}

// TestRunReadwriteTransaction_SetMessageDuringExecution verifies a message set
// at runtime via tx.Options().SetMessage drives the commit.
func TestRunReadwriteTransaction_SetMessageDuringExecution(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)

	const msg = "set during execution"
	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		if err := tx.Set(ctx, franceRecord()); err != nil {
			return err
		}
		tx.Options().SetMessage(msg)
		return nil
	}); err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}

	if got := git(t, root, "log", "-1", "--pretty=%s"); got != msg {
		t.Errorf("commit subject: got %q, want %q", got, msg)
	}
}

// TestRunReadwriteTransaction_NonGitDirNoError verifies that a message in a
// non-git directory is a no-op (file written, no error) rather than failing.
func TestRunReadwriteTransaction_NonGitDirNoError(t *testing.T) {
	ctx := context.Background()
	db, _ := setupSingleRecordDB(t) // no gitInit: plain directory

	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}, dal.TxWithMessage("no git here")); err != nil {
		t.Fatalf("RunReadwriteTransaction in non-git dir should not error: %v", err)
	}

	got := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Fatalf("record should still be written: %v", err)
	}
}

// A state transition commonly writes a domain record, journal entry, and
// outbox record together. Pin the adapter's failure behaviour: an error after
// the first write must not leave any record behind for a later retry to
// mistake as a committed transition.
func TestRunReadwriteTransaction_RollsBackAllWrittenFilesOnWorkerFailure(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)

	err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		if err := tx.Set(ctx, franceRecord()); err != nil {
			return err
		}
		germany := record.NewRecordWithData(
			record.NewKeyWithID("countries", "germany"),
			map[string]any{"name": "Germany", "population": 83000000},
		)
		if err := tx.Set(ctx, germany); err != nil {
			return err
		}
		return errors.New("inject worker failure")
	})
	if err == nil || err.Error() != "inject worker failure" {
		t.Fatalf("RunReadwriteTransaction error = %v, want injected worker failure", err)
	}

	for _, key := range []string{"france", "germany"} {
		path := filepath.Join(root, "countries", "$records", key+".yaml")
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Errorf("%s exists after rollback: stat error = %v", key, statErr)
		}
	}
}

// TestRunReadwriteTransaction_WorkerFailurePreservesPreexistingIndex proves a
// failed transaction restores the worktree without using a broad index reset.
// In particular, a user's staged record and unrelated staged file survive
// exactly as they were before the worker started.
func TestRunReadwriteTransaction_WorkerFailurePreservesPreexistingIndex(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)
	git(t, root, "commit", "--allow-empty", "-m", "init")

	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(root, "countries", "$records", "france.yaml")
	if err := os.WriteFile(filepath.Join(root, "unrelated.txt"), []byte("keep staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", "--", recordPath, "unrelated.txt")
	indexBefore := gitIndexBytes(t, root)

	err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{"name": "France", "population": 1})); err != nil {
			return err
		}
		return errors.New("inject worker failure")
	})
	if err == nil {
		t.Fatal("worker failure unexpectedly succeeded")
	}
	if got := gitIndexBytes(t, root); string(got) != string(indexBefore) {
		t.Fatal("rollback changed pre-existing index bytes")
	}
	got := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Fatal(err)
	}
	if population := got.Data().(map[string]any)["population"]; population != 67000000 {
		t.Fatalf("rollback record population = %v, want 67000000", population)
	}
}

// TestRunReadwriteTransaction_CommitUsesOnlyTransactionPaths verifies that a
// message commit is built from an isolated index, so unrelated staging and a
// pre-staged version of the touched record cannot leak into the commit.
func TestRunReadwriteTransaction_CommitUsesOnlyTransactionPaths(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)
	git(t, root, "commit", "--allow-empty", "-m", "init")
	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(root, "countries", "$records", "france.yaml")
	if err := os.WriteFile(filepath.Join(root, "unrelated.txt"), []byte("must not commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", "--", recordPath, "unrelated.txt")
	indexBefore := gitIndexBytes(t, root)

	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{"name": "France", "population": 68000000}))
	}, dal.TxWithMessage("commit only France")); err != nil {
		t.Fatal(err)
	}
	if files := git(t, root, "show", "--name-only", "--pretty=format:", "HEAD"); strings.Contains(files, "unrelated.txt") {
		t.Fatalf("transaction commit included unrelated staged file:\n%s", files)
	}
	if got := gitIndexBytes(t, root); string(got) != string(indexBefore) {
		t.Fatal("commit changed caller index bytes")
	}
}

// TestRunReadwriteTransaction_CommitFailureRestoresWorktreeAndIndex verifies
// that a Git identity failure leaves the caller's pre-existing index byte-for-
// byte intact while rolling the record file back.
func TestRunReadwriteTransaction_CommitFailureRestoresWorktreeAndIndex(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)
	git(t, root, "commit", "--allow-empty", "-m", "init")
	if err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, franceRecord())
	}); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(root, "countries", "$records", "france.yaml")
	if err := os.WriteFile(filepath.Join(root, "unrelated.txt"), []byte("keep staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", "--", recordPath, "unrelated.txt")
	indexBefore := gitIndexBytes(t, root)
	git(t, root, "config", "user.name", "")

	err := db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{"name": "France", "population": 1}))
	}, dal.TxWithMessage("must fail"))
	if err == nil {
		t.Fatal("commit with empty user.name unexpectedly succeeded")
	}
	if got := gitIndexBytes(t, root); string(got) != string(indexBefore) {
		t.Fatal("failed commit changed caller index bytes")
	}
	got := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(ctx, got); err != nil {
		t.Fatal(err)
	}
	if population := got.Data().(map[string]any)["population"]; population != 67000000 {
		t.Fatalf("failed commit left changed record population = %v, want 67000000", population)
	}
}

// TestRunReadwriteTransaction_GlobalLockKeepsReadersFromMixedState holds a
// multi-record write open after its first mutation. A DB-level reader must
// remain blocked, then observe both records after the transaction releases.
func TestRunReadwriteTransaction_GlobalLockKeepsReadersFromMixedState(t *testing.T) {
	ctx := context.Background()
	db, root := setupSingleRecordDB(t)
	gitInit(t, root)
	seed := func(id string, population int) error {
		return db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", id), map[string]any{"name": id, "population": population}))
		})
	}
	if err := seed("france", 1); err != nil {
		t.Fatal(err)
	}
	if err := seed("germany", 1); err != nil {
		t.Fatal(err)
	}
	firstWritten := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- db.RunReadwriteTransaction(ctx, func(_ context.Context, tx dal.ReadwriteTransaction) error {
			if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{"name": "france", "population": 2})); err != nil {
				return err
			}
			close(firstWritten)
			<-release
			return tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("countries", "germany"), map[string]any{"name": "germany", "population": 2}))
		})
	}()
	<-firstWritten
	readerDone := make(chan error, 1)
	france := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	germany := record.NewRecordWithData(record.NewKeyWithID("countries", "germany"), map[string]any{})
	go func() { readerDone <- db.GetMulti(ctx, []record.Record{france, germany}) }()
	select {
	case err := <-readerDone:
		t.Fatalf("reader observed transaction before it completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
	for _, got := range []record.Record{france, germany} {
		if population := got.Data().(map[string]any)["population"]; population != 2 {
			t.Fatalf("reader population = %v, want complete post-transaction state", population)
		}
	}
	if status := git(t, root, "status", "--short"); strings.Contains(status, ".dalgo2ingitdb.transaction.lock") {
		t.Fatalf("transaction lock polluted worktree status: %s", status)
	}
}
