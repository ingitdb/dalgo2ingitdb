package dalgo2ingitdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dal-go/dalgo/access"
	"gopkg.in/yaml.v3"
)

const (
	accessConfigDir       = ".ingitdb/access"
	accessManifestName    = "manifest.yaml"
	maxAccessManifestSize = 64 << 10
)

// readAccessManifest reads the owner-controlled ACL selection. Absence keeps
// legacy projects working; once the file exists, every malformed state is an
// error so a broken policy configuration cannot silently disable enforcement.
func readAccessManifest(projectPath string) (config access.FilePolicyConfig, present bool, err error) {
	ingitDir := filepath.Join(projectPath, ".ingitdb")
	accessDir := filepath.Join(projectPath, accessConfigDir)
	for _, dir := range []string{ingitDir, accessDir} {
		info, statErr := os.Lstat(dir)
		if errors.Is(statErr, os.ErrNotExist) {
			return config, false, nil
		}
		if statErr != nil {
			return config, false, fmt.Errorf("dalgo2ingitdb: inspect access configuration directory: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return config, true, fmt.Errorf("dalgo2ingitdb: access configuration directory %q must not be a symlink", dir)
		}
		if !info.IsDir() {
			return config, true, fmt.Errorf("dalgo2ingitdb: access configuration path %q must be a directory", dir)
		}
	}

	name := filepath.Join(accessDir, accessManifestName)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return config, true, errors.New("dalgo2ingitdb: access configuration directory exists but manifest.yaml is missing")
	}
	if err != nil {
		return config, false, fmt.Errorf("dalgo2ingitdb: inspect access manifest: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return config, true, fmt.Errorf("dalgo2ingitdb: access manifest must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return config, true, fmt.Errorf("dalgo2ingitdb: access manifest must be a regular file")
	}
	if info.Size() > maxAccessManifestSize {
		return config, true, fmt.Errorf("dalgo2ingitdb: access manifest exceeds %d bytes", maxAccessManifestSize)
	}
	file, err := os.Open(name)
	if err != nil {
		return config, true, fmt.Errorf("dalgo2ingitdb: open access manifest: %w", err)
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil {
		return config, true, fmt.Errorf("dalgo2ingitdb: inspect opened access manifest: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return config, true, errors.New("dalgo2ingitdb: access manifest changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxAccessManifestSize+1))
	if err != nil {
		return config, true, fmt.Errorf("dalgo2ingitdb: read access manifest: %w", err)
	}
	if len(data) > maxAccessManifestSize {
		return config, true, fmt.Errorf("dalgo2ingitdb: access manifest exceeds %d bytes", maxAccessManifestSize)
	}
	var document struct {
		Enabled    *bool    `yaml:"enabled"`
		Database   string   `yaml:"database"`
		Realm      string   `yaml:"realm"`
		Generation string   `yaml:"generation"`
		Policies   []string `yaml:"policies"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil {
		return config, true, fmt.Errorf("dalgo2ingitdb: decode access manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple YAML documents are not allowed")
		}
		return config, true, fmt.Errorf("dalgo2ingitdb: decode access manifest: %w", err)
	}
	if document.Enabled == nil {
		return config, true, errors.New("dalgo2ingitdb: access manifest requires explicit enabled")
	}
	config = access.FilePolicyConfig{Enabled: *document.Enabled, Database: document.Database, Realm: document.Realm, Policies: document.Policies}
	if document.Generation != "" {
		if !isSHA256(document.Generation) {
			return config, true, errors.New("dalgo2ingitdb: invalid access generation revision")
		}
		base := filepath.Join(accessDir, "generations", document.Generation)
		generation, err := readAndVerifyGeneration(base, document.Generation)
		if err != nil {
			return config, true, fmt.Errorf("dalgo2ingitdb: verify active access generation: %w", err)
		}
		if generation.Enabled != *document.Enabled || generation.Database != document.Database || generation.Realm != document.Realm || len(generation.Policies) != len(config.Policies) {
			return config, true, errors.New("dalgo2ingitdb: active manifest does not match its generation")
		}
		for i, policy := range config.Policies {
			prefix := filepath.ToSlash(filepath.Join("generations", document.Generation, "policies")) + "/"
			expected := prefix + generation.Policies[i].ID + ".yaml"
			if filepath.ToSlash(policy) != expected {
				return config, true, errors.New("dalgo2ingitdb: active generation policy path is outside its generation")
			}
		}
	}
	if config.Enabled && len(config.Policies) == 0 {
		return config, true, errors.New("dalgo2ingitdb: enabled access manifest requires at least one policy")
	}
	return config, true, nil
}
