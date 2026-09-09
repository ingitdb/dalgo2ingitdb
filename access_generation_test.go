package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

func TestOwnerPolicyControllerPublishesCommittedGenerationAndRecovers(t *testing.T) {
	_, root := setupSingleRecordDB(t)
	writeYAMLRecord(t, root, "countries", "france", "name: France\n")
	initGitRepository(t, root)
	controller, err := dalgo2ingitdb.NewOwnerPolicyController(root)
	if err != nil {
		t.Fatal(err)
	}
	policy := []byte(`apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: readers}
target: {database: world}
composition: dalgo-hierarchical-v1
default: deny
scopes:
  - path: /countries/france
    rules:
      - {id: read, effect: allow, operations: [get]}
`)
	publication, err := controller.Publish(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Realm: "staff.example", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: policy}}}, "", "publish owner policy")
	if err != nil {
		t.Fatal(err)
	}
	if publication.Revision == "" || publication.GitCommit == "" {
		t.Fatalf("publication = %+v", publication)
	}
	snapshot, err := controller.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != publication.Revision || snapshot.Config.Realm != "staff.example" || len(snapshot.Policies) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := access.NewPrincipal(access.PrincipalRef{Realm: "staff.example", Kind: access.PrincipalKindUser, ID: "u1"}, nil, nil)
	rec := record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(access.WithPrincipal(context.Background(), principal), rec); err != nil {
		t.Fatal(err)
	}
	manager, ok := db.(interface {
		PublishOwnerPolicyGeneration(context.Context, dalgo2ingitdb.OwnerPolicyGeneration, string, string) (dalgo2ingitdb.OwnerPolicyPublication, error)
	})
	if !ok {
		t.Fatal("generation database lost owner policy controller")
	}
	deny := []byte(`apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: readers}
target: {database: world}
composition: dalgo-hierarchical-v1
default: deny
scopes:
  - path: /countries/france
    rules:
      - {id: deny, effect: deny, operations: [get]}
`)
	second, err := manager.PublishOwnerPolicyGeneration(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Realm: "staff.example", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: deny}}}, publication.Revision, "deny reads")
	if err != nil {
		t.Fatal(err)
	}
	rec = record.NewRecordWithData(record.NewKeyWithID("countries", "france"), map[string]any{})
	if err := db.Get(access.WithPrincipal(context.Background(), principal), rec); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("live generation not activated: %v", err)
	}
	publication = second

	// HEAD is authoritative after the policy commit: a missing working pointer
	// is reconstructed from committed blobs on the next open.
	if err := os.Remove(filepath.Join(root, ".ingitdb", "access", "manifest.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader()); err != nil {
		t.Fatalf("recover committed generation: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".ingitdb", "access", "manifest.yaml")); err != nil {
		t.Fatal(err)
	}

	if _, err := controller.Publish(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: policy}}}, "stale", "stale"); err == nil {
		t.Fatal("stale revision accepted")
	}
}

func TestOwnerPolicyControllerRejectsUnsafeIDAndDirtyPolicyTree(t *testing.T) {
	root := t.TempDir()
	initGitRepository(t, root)
	controller, _ := dalgo2ingitdb.NewOwnerPolicyController(root)
	unsafe := []byte(`apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: ../escape}
target: {database: world}
composition: dalgo-hierarchical-v1
default: deny
scopes: [{path: /, rules: [{id: read, effect: allow, operations: [get]}]}]
`)
	_, err := controller.Publish(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: unsafe}}}, "", "unsafe")
	if err == nil {
		t.Fatal("unsafe policy ID accepted")
	}
	if err := os.MkdirAll(filepath.Join(root, ".ingitdb", "access"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".ingitdb", "access", "junk"), []byte("dirty"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err = controller.Publish(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: unsafe}}}, "", "dirty")
	if err == nil {
		t.Fatal("dirty policy tree accepted")
	}
}

func TestOwnerPolicyReloadRejectsDirtyCommittedGenerationWithoutRepair(t *testing.T) {
	root := t.TempDir()
	initGitRepository(t, root)
	controller, _ := dalgo2ingitdb.NewOwnerPolicyController(root)
	policy := []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: r, effect: allow, operations: [get]}]}]\n")
	published, err := controller.Publish(context.Background(), dalgo2ingitdb.OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []dalgo2ingitdb.OwnerPolicyDocument{{YAML: policy}}}, "", "publish")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".ingitdb", "access", "generations", published.Revision, "policies", "p.yaml")
	dirty := []byte("corrupt\n")
	if err := os.WriteFile(path, dirty, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Reload(context.Background()); err == nil {
		t.Fatal("dirty generation was silently repaired")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(dirty) {
		t.Fatalf("dirty bytes overwritten: %q %v", got, err)
	}
}

func initGitRepository(t *testing.T, root string) {
	t.Helper()
	commands := [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.invalid"}, {"add", "."}, {"commit", "--allow-empty", "-m", "initial"}}
	for _, args := range commands {
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
}
