package dalgo2ingitdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

// restoreSnapshots puts every touched record file back exactly as it was at
// transaction start. Restoring in a deterministic order makes failure
// diagnosis reproducible; the adapter is single-writer, so no concurrent
// writer can legitimately replace one of these paths between snapshot and
// rollback.
func restoreSnapshots(snapshots map[string]fileSnapshot) error {
	paths := make([]string, 0, len(snapshots))
	for path := range snapshots {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		snapshot := snapshots[path]
		if !snapshot.exists {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove created path %s: %w", path, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create rollback parent %s: %w", path, err)
		}
		if err := os.WriteFile(path, snapshot.data, snapshot.mode.Perm()); err != nil {
			return fmt.Errorf("restore %s: %w", path, err)
		}
	}
	return nil
}

// gitUnstagePaths removes only paths this transaction staged after a failed
// commit. A caller must use a clean single-writer worktree; we do not reset
// the whole index because unrelated user staging must never be discarded.
func gitUnstagePaths(ctx context.Context, repoDir string, paths []string) error {
	if !isInsideGitWorkTree(ctx, repoDir) {
		return nil
	}
	paths = dedupeStrings(paths)
	if len(paths) == 0 {
		return nil
	}
	args := append([]string{"-C", repoDir, "reset", "--"}, paths...)
	if out, err := exec.CommandContext(ctx, "git", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("dalgo2ingitdb: git reset changed paths: %w: %s", err, out)
	}
	return nil
}

// gitCommitPaths stages exactly the given record-file paths in the git
// repository that contains repoDir and commits them with message. It is used
// by RunReadwriteTransaction for the opt-in commit triggered by a transaction
// message. Paths are deduplicated (a record written several times in one
// transaction yields one staged path). git is invoked with -C repoDir so the
// enclosing repository is discovered even when repoDir is a subdirectory.
func gitCommitPaths(ctx context.Context, repoDir string, paths []string, message string) error {
	staged := dedupeStrings(paths)
	if len(staged) == 0 {
		return nil
	}
	// The transaction message is also a general annotation (e.g. for logging),
	// not necessarily a request to commit. Only commit when repoDir is inside a
	// git work tree; otherwise the message simply has no commit target and the
	// written files are left in place. This keeps dalgo2ingitdb usable on plain
	// directories (and in tests) without requiring a git repository.
	if !isInsideGitWorkTree(ctx, repoDir) {
		return nil
	}
	// Tracked paths are relative to the process working directory (they are
	// built from the projectPath the Database was opened with), while git is
	// invoked with -C repoDir. Absolute pathspecs resolve correctly in both
	// worlds.
	for i, p := range staged {
		if abs, err := filepath.Abs(p); err == nil {
			staged[i] = abs
		}
	}
	addArgs := append([]string{"-C", repoDir, "add", "--"}, staged...)
	if out, err := exec.CommandContext(ctx, "git", addArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("dalgo2ingitdb: git add: %w: %s", err, out)
	}
	out, err := exec.CommandContext(ctx, "git", "-C", repoDir, "commit", "-m", message).CombinedOutput()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: git commit: %w: %s", err, out)
	}
	return nil
}

// isInsideGitWorkTree reports whether dir is inside a git work tree.
func isInsideGitWorkTree(ctx context.Context, dir string) bool {
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--is-inside-work-tree")
	return cmd.Run() == nil
}

// dedupeStrings returns the input with duplicates removed, preserving order.
func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
