package dalgo2ingitdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/dal-go/dalgo/access"
	"gopkg.in/yaml.v3"
)

const generationAPIVersion = "ingitdb.org/access-generation/v1"

var safePolicyID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var ownerPolicyPublicationHook = func(string) error { return nil }

// OwnerPolicyDocument is one source document in a complete owner generation.
type OwnerPolicyDocument struct{ YAML []byte }

// OwnerPolicyGeneration is the complete candidate published atomically.
type OwnerPolicyGeneration struct {
	Enabled  bool
	Database string
	Realm    string
	Policies []OwnerPolicyDocument
}

// OwnerPolicyPublication is the committed result of a publication.
type OwnerPolicyPublication struct{ Revision, GitCommit string }

// OwnerPolicySnapshot is a complete compiled generation ready for atomic
// installation by the database enforcement facade.
type OwnerPolicySnapshot struct {
	Revision string
	Config   access.FilePolicyConfig
	Policies []access.Policy
}

// OwnerPolicyController publishes immutable owner-policy generations.
type OwnerPolicyController struct{ projectPath string }

func NewOwnerPolicyController(projectPath string) (*OwnerPolicyController, error) {
	if info, err := os.Stat(projectPath); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("dalgo2ingitdb: invalid owner policy project: %w", err)
	}
	return &OwnerPolicyController{projectPath: projectPath}, nil
}

// Reload recovers the Git-authoritative generation into the working tree and
// compiles one complete snapshot. It never exposes an underlying database.
func (c *OwnerPolicyController) Reload(ctx context.Context) (OwnerPolicySnapshot, error) {
	lock, err := transactionLockPath(ctx, c.projectPath)
	if err != nil {
		return OwnerPolicySnapshot{}, err
	}
	var snapshot OwnerPolicySnapshot
	err = withExclusiveLock(lock, func() error {
		if err := recoverCommittedGenerationLocked(ctx, c.projectPath); err != nil {
			return err
		}
		config, present, err := readAccessManifest(c.projectPath)
		if err != nil {
			return err
		}
		if !present || !config.Enabled {
			return errors.New("dalgo2ingitdb: no enabled owner policy generation")
		}
		revision, err := committedGenerationRevision(ctx, c.projectPath)
		if err != nil || revision == "" {
			return errors.New("dalgo2ingitdb: owner policy generation is not committed")
		}
		policies, err := access.LoadPolicyFiles(filepath.Join(c.projectPath, accessConfigDir), config)
		if err != nil {
			return err
		}
		snapshot = OwnerPolicySnapshot{Revision: revision, Config: config, Policies: policies}
		return nil
	})
	return snapshot, err
}

func (c *OwnerPolicyController) ActiveRevision(ctx context.Context) (string, error) {
	return committedGenerationRevision(ctx, c.projectPath)
}

func workingGenerationRevision(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, accessConfigDir, accessManifestName))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var active activeGenerationManifest
	if err := yaml.Unmarshal(data, &active); err != nil {
		return "", nil
	}
	if active.Generation != "" && !isSHA256(active.Generation) {
		return "", errors.New("dalgo2ingitdb: invalid working access generation")
	}
	return active.Generation, nil
}

// recoverCommittedGeneration makes the working policy subtree reflect the
// complete generation selected by Git HEAD. Flat manifests are deliberately
// ignored: Git-authoritative recovery is opt-in with the generation field.
func recoverCommittedGeneration(ctx context.Context, root string) error {
	if !isInsideGitWorkTree(ctx, root) {
		return nil
	}
	lock, err := transactionLockPath(ctx, root)
	if err != nil {
		return err
	}
	return withExclusiveLock(lock, func() error {
		return recoverCommittedGenerationLocked(ctx, root)
	})
}

func recoverCommittedGenerationLocked(ctx context.Context, root string) error {
	manifestPath := filepath.ToSlash(filepath.Join(accessConfigDir, accessManifestName))
	committed, err := gitBlob(ctx, root, manifestPath)
	if err != nil {
		return nil
	}
	var active activeGenerationManifest
	decoder := yaml.NewDecoder(bytes.NewReader(committed))
	decoder.KnownFields(true)
	if err := decoder.Decode(&active); err != nil || active.Generation == "" {
		return nil
	}
	if !isSHA256(active.Generation) {
		return errors.New("dalgo2ingitdb: committed access generation is invalid")
	}
	genPath := filepath.ToSlash(filepath.Join(accessConfigDir, "generations", active.Generation, "manifest.yaml"))
	genBytes, err := gitBlob(ctx, root, genPath)
	if err != nil {
		return fmt.Errorf("dalgo2ingitdb: read committed generation: %w", err)
	}
	if sha256Text(genBytes) != active.Generation {
		return errors.New("dalgo2ingitdb: committed generation digest mismatch")
	}
	var generation generationManifest
	if err := yaml.Unmarshal(genBytes, &generation); err != nil {
		return err
	}
	blobs := map[string][]byte{genPath: genBytes}
	for _, policy := range generation.Policies {
		if !safePolicyID.MatchString(policy.ID) || policy.File != "policies/"+policy.ID+".yaml" {
			return errors.New("dalgo2ingitdb: unsafe committed policy reference")
		}
		path := filepath.ToSlash(filepath.Join(accessConfigDir, "generations", active.Generation, policy.File))
		data, err := gitBlob(ctx, root, path)
		if err != nil {
			return err
		}
		doc, err := access.ParseDTQLPolicy(data)
		if err != nil {
			return err
		}
		canonical, _ := access.MarshalDTQLPolicyJSON(doc)
		if doc.Metadata.Name != policy.ID || sha256Text(canonical) != policy.Digest {
			return fmt.Errorf("dalgo2ingitdb: committed policy %q mismatch", policy.ID)
		}
		blobs[path] = data
	}
	working := filepath.Join(root, filepath.FromSlash(manifestPath))
	current, readErr := os.ReadFile(working)
	if readErr == nil && !bytes.Equal(current, committed) {
		var old activeGenerationManifest
		if yaml.Unmarshal(current, &old) != nil || !isSHA256(old.Generation) || verifyGeneration(filepath.Join(root, accessConfigDir, "generations", old.Generation), old.Generation) != nil {
			return errors.New("dalgo2ingitdb: dirty access manifest is not a recoverable committed generation")
		}
	} else if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	generationBase := filepath.Join(root, accessConfigDir, "generations", active.Generation)
	if info, statErr := os.Lstat(generationBase); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("dalgo2ingitdb: committed generation path is not a safe directory")
		}
		for path, want := range blobs {
			got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
			if err != nil || !bytes.Equal(got, want) {
				return fmt.Errorf("dalgo2ingitdb: committed generation working file %q is dirty or corrupt", path)
			}
		}
	} else if errors.Is(statErr, os.ErrNotExist) {
		if err := materializeCommittedGeneration(root, active.Generation, blobs); err != nil {
			return err
		}
	} else {
		return statErr
	}
	if bytes.Equal(current, committed) {
		return nil
	}
	if err := rejectSymlinkAncestors(root, filepath.Dir(working)); err != nil {
		return err
	}
	return atomicWriteFile(working, committed, 0644)
}

func materializeCommittedGeneration(root, revision string, blobs map[string][]byte) error {
	parent := filepath.Join(root, accessConfigDir, "generations")
	if err := rejectSymlinkAncestors(root, parent); err != nil {
		return err
	}
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".generation-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	prefix := filepath.ToSlash(filepath.Join(accessConfigDir, "generations", revision)) + "/"
	for path, data := range blobs {
		rel := strings.TrimPrefix(path, prefix)
		if rel == path {
			return errors.New("dalgo2ingitdb: committed generation path escaped")
		}
		if err := atomicWriteFile(filepath.Join(tmp, filepath.FromSlash(rel)), data, 0644); err != nil {
			return err
		}
	}
	if err := syncDir(tmp); err != nil {
		return err
	}
	dest := filepath.Join(parent, revision)
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	return syncDir(parent)
}

func rejectSymlinkAncestors(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return errors.New("dalgo2ingitdb: path escapes project")
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("dalgo2ingitdb: symlink ancestor %q", current)
		}
	}
	return nil
}

func gitBlob(ctx context.Context, root, path string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, "git", "-C", root, "show", "HEAD:"+path).Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}

type generationPolicy struct {
	ID     string `yaml:"id"`
	File   string `yaml:"file"`
	Digest string `yaml:"digest"`
}
type generationManifest struct {
	APIVersion string             `yaml:"apiVersion"`
	Enabled    bool               `yaml:"enabled"`
	Database   string             `yaml:"database"`
	Realm      string             `yaml:"realm,omitempty"`
	Policies   []generationPolicy `yaml:"policies"`
}
type activeGenerationManifest struct {
	Enabled    *bool    `yaml:"enabled"`
	Database   string   `yaml:"database"`
	Realm      string   `yaml:"realm,omitempty"`
	Generation string   `yaml:"generation"`
	Policies   []string `yaml:"policies"`
}

// Publish writes a complete immutable generation and advances Git HEAD with an
// expected active-generation comparison. expectedRevision is empty only for a
// repository that has no active generation yet.
func (c *OwnerPolicyController) Publish(ctx context.Context, candidate OwnerPolicyGeneration, expectedRevision, message string) (OwnerPolicyPublication, error) {
	lock, err := transactionLockPath(ctx, c.projectPath)
	if err != nil {
		return OwnerPolicyPublication{}, err
	}
	var result OwnerPolicyPublication
	err = withExclusiveLock(lock, func() error {
		oldHead, hasHead, err := gitHead(ctx, c.projectPath)
		if err != nil {
			return err
		}
		if !hasHead {
			oldHead = ""
		}
		current, err := committedGenerationRevisionAt(ctx, c.projectPath, oldHead)
		if err != nil {
			return err
		}
		if current != expectedRevision {
			return fmt.Errorf("dalgo2ingitdb: owner policy revision conflict: expected %q, current %q", expectedRevision, current)
		}
		if err := requireCleanAccessTree(ctx, c.projectPath); err != nil {
			return err
		}
		manifest, sources, revision, err := buildGeneration(candidate)
		if err != nil {
			return err
		}
		paths, err := materializeGeneration(c.projectPath, revision, manifest, sources)
		if err != nil {
			return err
		}
		if err := ownerPolicyPublicationHook("generation_materialized"); err != nil {
			return err
		}
		active := activeGenerationManifest{Enabled: &candidate.Enabled, Database: candidate.Database, Realm: candidate.Realm, Generation: revision}
		for _, policy := range manifest.Policies {
			active.Policies = append(active.Policies, filepath.ToSlash(filepath.Join("generations", revision, policy.File)))
		}
		activeBytes, err := yaml.Marshal(active)
		if err != nil {
			return err
		}
		commit, err := gitCommitPolicyCAS(ctx, c.projectPath, paths, activeBytes, message, oldHead)
		if err != nil {
			return err
		}
		if err := ownerPolicyPublicationHook("head_committed"); err != nil {
			return err
		}
		activePath := filepath.Join(c.projectPath, accessConfigDir, accessManifestName)
		if err := rejectSymlinkAncestors(c.projectPath, filepath.Dir(activePath)); err != nil {
			return err
		}
		if err := atomicWriteFile(activePath, activeBytes, 0o644); err != nil {
			return fmt.Errorf("dalgo2ingitdb: policy committed but working activation needs recovery: %w", err)
		}
		if err := ownerPolicyPublicationHook("working_pointer_activated"); err != nil {
			return err
		}
		result = OwnerPolicyPublication{Revision: revision, GitCommit: commit}
		return nil
	})
	return result, err
}

func buildGeneration(candidate OwnerPolicyGeneration) (generationManifest, map[string][]byte, string, error) {
	if candidate.Database == "" || !candidate.Enabled || len(candidate.Policies) == 0 {
		return generationManifest{}, nil, "", errors.New("dalgo2ingitdb: enabled generation requires database and policies")
	}
	if len(candidate.Policies) > 100 {
		return generationManifest{}, nil, "", errors.New("dalgo2ingitdb: owner generation exceeds 100 policies")
	}
	if candidate.Realm != "" && (strings.TrimSpace(candidate.Realm) != candidate.Realm || len(candidate.Realm) > 256) {
		return generationManifest{}, nil, "", errors.New("dalgo2ingitdb: owner generation realm must be canonical and at most 256 bytes")
	}
	manifest := generationManifest{APIVersion: generationAPIVersion, Enabled: true, Database: candidate.Database, Realm: candidate.Realm}
	sources := map[string][]byte{}
	for _, item := range candidate.Policies {
		doc, err := access.ParseDTQLPolicy(item.YAML)
		if err != nil {
			return generationManifest{}, nil, "", err
		}
		id := doc.Metadata.Name
		if !safePolicyID.MatchString(id) {
			return generationManifest{}, nil, "", fmt.Errorf("dalgo2ingitdb: unsafe policy id %q", id)
		}
		if doc.Target.Database != candidate.Database {
			return generationManifest{}, nil, "", fmt.Errorf("dalgo2ingitdb: policy %q targets wrong database", id)
		}
		canonical, err := access.MarshalDTQLPolicyJSON(doc)
		if err != nil {
			return generationManifest{}, nil, "", err
		}
		digest := sha256Text(canonical)
		file := filepath.ToSlash(filepath.Join("policies", id+".yaml"))
		if _, duplicate := sources[file]; duplicate {
			return generationManifest{}, nil, "", fmt.Errorf("dalgo2ingitdb: duplicate policy id %q", id)
		}
		source, err := access.MarshalDTQLPolicyYAML(doc)
		if err != nil {
			return generationManifest{}, nil, "", err
		}
		sources[file] = source
		manifest.Policies = append(manifest.Policies, generationPolicy{ID: id, File: file, Digest: digest})
	}
	sort.Slice(manifest.Policies, func(i, j int) bool { return manifest.Policies[i].ID < manifest.Policies[j].ID })
	encoded, err := yaml.Marshal(manifest)
	if err != nil {
		return generationManifest{}, nil, "", err
	}
	return manifest, sources, sha256Text(encoded), nil
}

func materializeGeneration(root, revision string, manifest generationManifest, sources map[string][]byte) ([]string, error) {
	base := filepath.Join(root, accessConfigDir, "generations", revision)
	manifestBytes, _ := yaml.Marshal(manifest)
	if info, err := os.Lstat(base); err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		if err := verifyGeneration(base, revision); err != nil {
			return nil, fmt.Errorf("dalgo2ingitdb: existing generation invalid: %w", err)
		}
		paths := []string{filepath.Join(base, "manifest.yaml")}
		for _, p := range manifest.Policies {
			paths = append(paths, filepath.Join(base, filepath.FromSlash(p.File)))
		}
		return paths, nil
	} else if err == nil {
		return nil, errors.New("dalgo2ingitdb: generation path must be a non-symlink directory")
	}
	parent := filepath.Dir(base)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return nil, err
	}
	tmp, err := os.MkdirTemp(parent, ".generation-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	paths := make([]string, 0, len(sources)+1)
	for name, data := range sources {
		p := filepath.Join(tmp, filepath.FromSlash(name))
		if err := atomicWriteFile(p, data, 0644); err != nil {
			return nil, err
		}
		paths = append(paths, filepath.Join(base, filepath.FromSlash(name)))
	}
	if err := atomicWriteFile(filepath.Join(tmp, "manifest.yaml"), manifestBytes, 0644); err != nil {
		return nil, err
	}
	paths = append(paths, filepath.Join(base, "manifest.yaml"))
	if err := syncDir(tmp); err != nil {
		return nil, err
	}
	if err := ownerPolicyPublicationHook("generation_files_synced"); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, base); err != nil {
		return nil, err
	}
	if err := ownerPolicyPublicationHook("generation_renamed"); err != nil {
		return nil, err
	}
	if err := syncDir(parent); err != nil {
		return nil, err
	}
	if err := ownerPolicyPublicationHook("generation_parent_synced"); err != nil {
		return nil, err
	}
	return paths, nil
}

func verifyGeneration(base, revision string) error {
	_, err := readAndVerifyGeneration(base, revision)
	return err
}

func readAndVerifyGeneration(base, revision string) (generationManifest, error) {
	baseInfo, err := os.Lstat(base)
	if err != nil || !baseInfo.IsDir() || baseInfo.Mode()&os.ModeSymlink != 0 {
		return generationManifest{}, errors.New("generation must be a non-symlink directory")
	}
	manifestPath := filepath.Join(base, "manifest.yaml")
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil || !manifestInfo.Mode().IsRegular() || manifestInfo.Mode()&os.ModeSymlink != 0 {
		return generationManifest{}, errors.New("generation manifest must be a regular non-symlink file")
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return generationManifest{}, err
	}
	if sha256Text(data) != revision {
		return generationManifest{}, errors.New("generation digest mismatch")
	}
	var m generationManifest
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&m); err != nil {
		return generationManifest{}, err
	}
	if m.APIVersion != generationAPIVersion {
		return generationManifest{}, errors.New("unsupported generation apiVersion")
	}
	policiesInfo, err := os.Lstat(filepath.Join(base, "policies"))
	if err != nil || !policiesInfo.IsDir() || policiesInfo.Mode()&os.ModeSymlink != 0 {
		return generationManifest{}, errors.New("generation policies must be a non-symlink directory")
	}
	for _, p := range m.Policies {
		if !safePolicyID.MatchString(p.ID) || p.File != "policies/"+p.ID+".yaml" {
			return generationManifest{}, errors.New("unsafe generation policy reference")
		}
		policyPath := filepath.Join(base, filepath.FromSlash(p.File))
		info, err := os.Lstat(policyPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return generationManifest{}, fmt.Errorf("policy %q must be a regular non-symlink file", p.ID)
		}
		source, err := os.ReadFile(policyPath)
		if err != nil {
			return generationManifest{}, err
		}
		doc, err := access.ParseDTQLPolicy(source)
		if err != nil {
			return generationManifest{}, err
		}
		canonical, _ := access.MarshalDTQLPolicyJSON(doc)
		if doc.Metadata.Name != p.ID || sha256Text(canonical) != p.Digest {
			return generationManifest{}, fmt.Errorf("policy %q digest or id mismatch", p.ID)
		}
	}
	return m, nil
}

func committedGenerationRevision(ctx context.Context, root string) (string, error) {
	return committedGenerationRevisionAt(ctx, root, "HEAD")
}

func committedGenerationRevisionAt(ctx context.Context, root, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	out, err := exec.CommandContext(ctx, "git", "-C", root, "show", ref+":"+filepath.ToSlash(filepath.Join(accessConfigDir, accessManifestName))).Output()
	if err != nil {
		return "", nil
	}
	var m activeGenerationManifest
	if yaml.Unmarshal(out, &m) != nil {
		return "", nil
	}
	return m.Generation, nil
}

func requireCleanAccessTree(ctx context.Context, root string) error {
	out, err := exec.CommandContext(ctx, "git", "-C", root, "ls-tree", "-r", "--name-only", "HEAD", "--", accessConfigDir).Output()
	if err != nil {
		return err
	}
	tracked := map[string]bool{}
	for _, path := range strings.Fields(string(out)) {
		tracked[path] = true
		head, err := gitBlob(ctx, root, path)
		if err != nil {
			return err
		}
		working, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil || !bytes.Equal(head, working) {
			return fmt.Errorf("dalgo2ingitdb: policy/config path %q differs from HEAD", path)
		}
	}
	base := filepath.Join(root, accessConfigDir)
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	allowedGenerationPrefixes := []string{}
	genRoot := filepath.Join(base, "generations")
	if entries, readErr := os.ReadDir(genRoot); readErr == nil {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".generation-") {
				continue
			}
			if !isSHA256(entry.Name()) || verifyGeneration(filepath.Join(genRoot, entry.Name()), entry.Name()) != nil {
				continue
			}
			allowedGenerationPrefixes = append(allowedGenerationPrefixes, filepath.ToSlash(filepath.Join(accessConfigDir, "generations", entry.Name()))+"/")
		}
	}
	err = filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("dalgo2ingitdb: policy/config subtree contains symlink %q", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !tracked[rel] {
			for _, prefix := range allowedGenerationPrefixes {
				if strings.HasPrefix(rel, prefix) {
					return nil
				}
			}
			if strings.Contains(rel, "/generations/.generation-") {
				return nil
			}
			return fmt.Errorf("dalgo2ingitdb: policy/config subtree has untracked file %q", rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
func sha256Text(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func isSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}
func atomicWriteFile(name string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, name); err != nil {
		return err
	}
	return syncDir(filepath.Dir(name))
}
func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
func gitCommitPolicyCAS(ctx context.Context, root string, paths []string, active []byte, message, expectedHead string) (string, error) {
	index, err := os.CreateTemp("", "dalgo2ingitdb-policy-index-")
	if err != nil {
		return "", err
	}
	indexPath := index.Name()
	_ = index.Close()
	defer func() { _ = os.Remove(indexPath) }()
	env := append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	if expectedHead != "" {
		if out, err := run("read-tree", expectedHead); err != nil {
			return "", fmt.Errorf("read tree: %w: %s", err, out)
		}
	} else {
		if _, err := run("read-tree", "--empty"); err != nil {
			return "", err
		}
	}
	args := []string{"add", "--"}
	args = append(args, paths...)
	if out, err := run(args...); err != nil {
		return "", fmt.Errorf("stage policy generation: %w: %s", err, out)
	}
	blobCmd := exec.CommandContext(ctx, "git", "-C", root, "hash-object", "-w", "--stdin")
	blobCmd.Stdin = bytes.NewReader(active)
	blobOut, err := blobCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("store active manifest blob: %w: %s", err, blobOut)
	}
	activePath := filepath.ToSlash(filepath.Join(accessConfigDir, accessManifestName))
	if out, err := run("update-index", "--add", "--cacheinfo", "100644,"+strings.TrimSpace(string(blobOut))+","+activePath); err != nil {
		return "", fmt.Errorf("stage active manifest: %w: %s", err, out)
	}
	tree, err := run("write-tree")
	if err != nil {
		return "", err
	}
	cargs := []string{"commit-tree", strings.TrimSpace(string(tree))}
	if expectedHead != "" {
		cargs = append(cargs, "-p", expectedHead)
	}
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, cargs...)...)
	cmd.Env = env
	cmd.Stdin = strings.NewReader(message + "\n")
	commit, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("create policy commit: %w: %s", err, commit)
	}
	id := strings.TrimSpace(string(commit))
	if err := ownerPolicyPublicationHook("commit_created"); err != nil {
		return "", err
	}
	old := expectedHead
	if old == "" {
		old = strings.Repeat("0", 40)
	}
	if out, err := run("update-ref", "HEAD", id, old); err != nil {
		return "", fmt.Errorf("publish policy commit: %w: %s", err, out)
	}
	return id, nil
}
