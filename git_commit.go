package dalgo2ingitdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
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

// gitCommitPaths stages exactly the given record-file paths in the git
// repository that contains repoDir and commits them with message. It never
// changes the caller's real index: a disposable index starts from HEAD, adds
// only paths, and is committed with Git plumbing. This is essential because a
// normal `git commit` would include unrelated pre-staged changes, while a
// failure rollback/reset could erase them.
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
	index, err := os.CreateTemp("", "dalgo2ingitdb-index-*")
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: create temporary index: %w", err)
	}
	indexPath := index.Name()
	if err := index.Close(); err != nil {
		_ = os.Remove(indexPath)
		return fmt.Errorf("dalgo2ingitdb: close temporary index: %w", err)
	}
	defer func() { _ = os.Remove(indexPath) }()

	env := append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repoDir}, args...)...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}

	oldHead, hasHead, err := gitHead(ctx, repoDir)
	if err != nil {
		return err
	}
	if hasHead {
		if out, err := run("read-tree", oldHead); err != nil {
			return fmt.Errorf("dalgo2ingitdb: seed temporary index: %w: %s", err, out)
		}
	} else if out, err := run("read-tree", "--empty"); err != nil {
		return fmt.Errorf("dalgo2ingitdb: initialise temporary index: %w: %s", err, out)
	}
	addArgs := append([]string{"add", "--"}, staged...)
	if out, err := run(addArgs...); err != nil {
		return fmt.Errorf("dalgo2ingitdb: git add: %w: %s", err, out)
	}
	tree, err := run("write-tree")
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: write transaction tree: %w: %s", err, tree)
	}
	commitArgs := []string{"commit-tree", strings.TrimSpace(string(tree))}
	if hasHead {
		commitArgs = append(commitArgs, "-p", oldHead)
	}
	commit := exec.CommandContext(ctx, "git", append([]string{"-C", repoDir}, commitArgs...)...)
	commit.Env = env
	commit.Stdin = strings.NewReader(message + "\n")
	newHead, err := commit.CombinedOutput()
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: create transaction commit: %w: %s", err, newHead)
	}
	newHeadID := strings.TrimSpace(string(newHead))
	updateArgs := []string{"update-ref", "HEAD", newHeadID}
	if hasHead {
		updateArgs = append(updateArgs, oldHead)
	} else {
		updateArgs = append(updateArgs, strings.Repeat("0", 40))
	}
	if out, err := run(updateArgs...); err != nil {
		return fmt.Errorf("dalgo2ingitdb: publish transaction commit: %w: %s", err, out)
	}
	return nil
}

// gitHead returns the current commit ID. An unborn branch has no HEAD commit
// and is a supported starting point for the first transaction commit.
func gitHead(ctx context.Context, repoDir string) (string, bool, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", repoDir, "rev-parse", "--verify", "HEAD^{commit}").CombinedOutput()
	if err == nil {
		return strings.TrimSpace(string(out)), true, nil
	}
	if ctx.Err() != nil {
		return "", false, fmt.Errorf("dalgo2ingitdb: resolve Git HEAD: %w", ctx.Err())
	}
	message := string(out)
	if strings.Contains(message, "Needed a single revision") || strings.Contains(message, "unknown revision or path not in the working tree") {
		return "", false, nil
	}
	return "", false, fmt.Errorf("dalgo2ingitdb: resolve Git HEAD: %w: %s", err, out)
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
