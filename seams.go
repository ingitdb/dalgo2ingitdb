package dalgo2ingitdb

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"

	"gopkg.in/yaml.v3"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// Test seams over os.*/config functions. These hold no state; tests swap them
// to inject failures that are otherwise unreachable (e.g. mkdir/write failures,
// or os.Remove returning ErrNotExist via a TOCTOU race), then restore them.
// A test that swaps a seam must NOT call t.Parallel(), since the swap mutates
// package-level state shared with other tests.
var (
	// osMkdirAll is used by CreateCollection.
	osMkdirAll = os.MkdirAll
	// osReadFile is used by rewriteRecordFiles.
	osReadFile = os.ReadFile
	// osWriteFile is used by writeCollectionDefYAML.
	osWriteFile = os.WriteFile
	// osRemove is used by deleteSingleRecordFile.
	osRemove = os.Remove
	// osLstat is used by readAccessManifest and access_generation.
	osLstat = os.Lstat
	// filepathRel is used by readAllSingleRecords. The seam lets tests reach the
	// error branch, which in production is unreachable because the path argument
	// always comes from filepath.Glob under basePath, so it is always relative to
	// basePath and filepath.Rel never fails.
	filepathRel = filepath.Rel
	// filepathIsAbs is used by validateCollectionName. The seam lets tests reach
	// the absolute-path branch, which in production is unreachable because the
	// earlier path-segment check already rejects names that would be absolute.
	filepathIsAbs = filepath.IsAbs
	// regexpCompile is used by buildKeyExtractor. The seam lets tests reach the
	// compile-error branch, which in production is unreachable because the pattern
	// is assembled from regexp.QuoteMeta-escaped parts and is always valid.
	regexpCompile = regexp.Compile
	// yamlMarshal is used by writeCollectionDefYAML and rewriteRecordFiles.
	// The seam lets tests reach the marshal-error branches, which in production
	// are unreachable for the plain map/struct values passed.
	yamlMarshal = yaml.Marshal
	// writeRootCollections is used by the registry helpers.
	writeRootCollections = writeRootCollectionsYAML
	// readSingleRecord is used by readAllSingleRecords. The seam lets tests
	// reach the found==false branch, which in production only occurs when a
	// globbed file vanishes before the read (a TOCTOU race).
	readSingleRecord = readSingleRecordFile
	// newFileLocker is used by withSharedLock/withExclusiveLock. The seam lets
	// tests inject lock-acquisition failures.
	newFileLocker = defaultFileLocker
	// runtimeGOOS is used by validateRecordPathSegment to simulate different OS environments.
	runtimeGOOS = runtime.GOOS
	// lockFilePathSeam is used by defaultFileLocker to resolve lock file targets.
	lockFilePathSeam = lockFilePath
	// filepathAbs is used by sidecarLockPath, transactionLockPath, etc.
	filepathAbs = filepath.Abs
	// userCacheDir is used by transactionLockPath.
	userCacheDir = os.UserCacheDir
	// osCreateTemp is used by gitCommitPaths and gitCommitPolicyCAS.
	osCreateTemp = os.CreateTemp
	// gitCmdRun executes git commands in gitCommitPaths and gitCommitPolicyCAS.
	gitCmdRun = func(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	// gitShowBlob executes git show in gitBlob.
	gitShowBlob = func(ctx context.Context, root, path string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", "-C", root, "show", "HEAD:"+path).Output()
	}
	// readAllRecords reads all records from disk in foreign_keys.go and query.go.
	readAllRecords = readAllRecordsFromDisk
	// gitHeadSeam resolves Git HEAD in gitCommitPaths.
	gitHeadSeam = gitHead
	// readAndVerifyGenerationSeam reads and verifies access generation in readAccessManifest.
	readAndVerifyGenerationSeam = readAndVerifyGeneration
	// osStat is used by snapshot and ensureRealRootedScope.
	osStat = os.Stat
	// recoverCommittedGenerationSeam is used by NewDatabaseWithOptions.
	recoverCommittedGenerationSeam = recoverCommittedGeneration
	// workingGenerationRevisionSeam is used by NewDatabaseWithOptions.
	workingGenerationRevisionSeam = workingGenerationRevision
	// committedGenerationRevisionAtSeam is used by Publish.
	committedGenerationRevisionAtSeam = committedGenerationRevisionAt
	// newOwnerPolicyControllerSeam is used by NewDatabaseWithOptions.
	newOwnerPolicyControllerSeam = NewOwnerPolicyController
	// accessSecureDBSeam is used by NewDatabaseWithOptions.
	accessSecureDBSeam = access.SecureDB
	// transactionLockPathSeam is used by RunReadwriteTransaction and withTransactionReadLock.
	transactionLockPathSeam = transactionLockPath
	// restoreSnapshotsSeam is used by runReadwriteTransaction.
	restoreSnapshotsSeam = restoreSnapshots
	// dalIsTransform is used by applyFieldUpdate.
	dalIsTransform = dal.IsTransform
	// foreignKeyTargetExistsSeam is used by Delete.
	foreignKeyTargetExistsSeam = foreignKeyTargetExists
	// controllerPublishSeam is used by securedDatabase.PublishOwnerPolicyGeneration.
	controllerPublishSeam = func(c *OwnerPolicyController, ctx context.Context, candidate OwnerPolicyGeneration, expectedRevision, message string) (OwnerPolicyPublication, error) {
		return c.Publish(ctx, candidate, expectedRevision, message)
	}
	// controllerReloadSeam is used by NewDatabase and securedDatabase.
	controllerReloadSeam = func(c *OwnerPolicyController, ctx context.Context) (OwnerPolicySnapshot, error) {
		return c.Reload(ctx)
	}
	// osRename is used by atomicWriteFile and materializeGeneration.
	osRename = os.Rename
	// randReadSeam is used by configureProtected.
	randReadSeam = rand.Read
	// newValidatedCoordinatorSeam is used by configureProtected.
	newValidatedCoordinatorSeam = access.NewValidatedEnforcementCoordinator
	// validateProtectedDefSeam is used by protectedStorage.prepare.
	validateProtectedDefSeam = validateProtectedDefinition
	// dataToMapSeam is used by protectedStorage.prepare.
	dataToMapSeam = dalrecord.DataToMap
	// readMapOfRecordsFileSeam is used by assignBatchCandidateRevisions.
	readMapOfRecordsFileSeam = readMapOfRecordsFile
	// encodeRecordContentSeam is used by assignBatchCandidateRevisions.
	encodeRecordContentSeam = ingitdb.EncodeRecordContentForCollection
	// sameFileSeam is used by ensureTopLevelDir.
	sameFileSeam = os.SameFile
	// rootedJSONLBufferSizeSeam is used by recoverAndReadJSONL.
	rootedJSONLBufferSizeSeam = rootedJSONLBufferSize
	// syncRootDirectorySeam is used by ensureRealRootedScope.
	syncRootDirectorySeam = syncRootDirectory
	// openRootedFileSeam is used by openFile.
	openRootedFileSeam = func(root *os.Root, path string, flag int, perm os.FileMode) (*os.File, error) {
		return root.OpenFile(path, flag, perm)
	}
	// dirSyncSeam is used by syncRootDirectory.
	dirSyncSeam = func(f *os.File) error {
		return f.Sync()
	}
	// rootLstatSeam is used by ensureRealRootedScope.
	rootLstatSeam = func(root *os.Root, path string) (os.FileInfo, error) {
		return root.Lstat(path)
	}
	// gitCmdRunStdin executes git commands with stdin in gitCommitPolicyCAS.
	gitCmdRunStdin = func(ctx context.Context, dir string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
		cmd.Env = env
		cmd.Stdin = stdin
		return cmd.CombinedOutput()
	}
	// accessLoadPolicyFiles is used by OwnerPolicyController.Reload.
	accessLoadPolicyFiles = access.LoadPolicyFiles
	// syncDirSeam is used by access_generation directory sync operations.
	syncDirSeam = syncDir
	// accessMarshalDTQLPolicyJSON is used by buildGeneration.
	accessMarshalDTQLPolicyJSON = access.MarshalDTQLPolicyJSON
	// accessMarshalDTQLPolicyYAML is used by buildGeneration.
	accessMarshalDTQLPolicyYAML = access.MarshalDTQLPolicyYAML
	// filepathWalkDir is used by requireCleanAccessTree.
	filepathWalkDir = filepath.WalkDir
)
