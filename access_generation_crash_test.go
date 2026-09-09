package dalgo2ingitdb

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/record"
)

func TestOwnerPolicyPublicationCrashOrdering(t *testing.T) {
	policy := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: readers}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: read, effect: allow, operations: [get]}]}]\n")}
	replacement := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: readers}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: deny, effect: deny, operations: [get]}]}]\n")}
	preCAS := map[string]bool{"generation_files_synced": true, "generation_renamed": true, "generation_parent_synced": true, "generation_materialized": true, "commit_created": true}
	for _, phase := range []string{"generation_files_synced", "generation_renamed", "generation_parent_synced", "generation_materialized", "commit_created", "head_committed", "working_pointer_activated"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			runGitForGenerationTest(t, root, "init")
			runGitForGenerationTest(t, root, "config", "user.name", "Test")
			runGitForGenerationTest(t, root, "config", "user.email", "test@example.invalid")
			runGitForGenerationTest(t, root, "commit", "--allow-empty", "-m", "initial")
			controller, _ := NewOwnerPolicyController(root)
			initial, err := controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{policy}}, "", "initial policy")
			if err != nil {
				t.Fatal(err)
			}
			before, _, _ := gitHead(context.Background(), root)
			ownerPolicyPublicationHook = func(got string) error {
				if got == phase {
					return errors.New("injected crash")
				}
				return nil
			}
			defer func() { ownerPolicyPublicationHook = func(string) error { return nil } }()
			_, err = controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{replacement}}, initial.Revision, "publish")
			if err == nil {
				t.Fatal("injected failure missing")
			}
			after, _, _ := gitHead(context.Background(), root)
			if preCAS[phase] {
				if after != before {
					t.Fatal("HEAD changed before commit point")
				}
				ownerPolicyPublicationHook = func(string) error { return nil }
				snapshot, err := controller.Reload(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.Revision != initial.Revision {
					t.Fatal("pre-CAS crash activated new generation")
				}
				return
			}
			if after == before {
				t.Fatal("HEAD did not change at commit point")
			}
			ownerPolicyPublicationHook = func(string) error { return nil }
			if _, err := controller.Reload(context.Background()); err != nil {
				t.Fatalf("post-CAS recovery: %v", err)
			}
		})
	}
}

func TestOwnerPolicyPublicationProcessCrashOrdering(t *testing.T) {
	pre := map[string]bool{"generation_files_synced": true, "generation_renamed": true, "generation_parent_synced": true, "generation_materialized": true, "commit_created": true}
	for _, phase := range []string{"generation_files_synced", "generation_renamed", "generation_parent_synced", "generation_materialized", "commit_created", "head_committed", "working_pointer_activated", "snapshot_activated"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			runGitForGenerationTest(t, root, "init")
			runGitForGenerationTest(t, root, "config", "user.name", "Test")
			runGitForGenerationTest(t, root, "config", "user.email", "test@example.invalid")
			runGitForGenerationTest(t, root, "commit", "--allow-empty", "-m", "initial")
			controller, _ := NewOwnerPolicyController(root)
			policy := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: readers}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: read, effect: allow, operations: [get]}]}]\n")}
			initial, err := controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{policy}}, "", "initial")
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestOwnerPolicyPublicationCrashChild$")
			cmd.Env = append(os.Environ(), "INGITDB_CRASH_ROOT="+root, "INGITDB_CRASH_PHASE="+phase, "INGITDB_CRASH_EXPECTED="+initial.Revision)
			if err := cmd.Run(); err == nil {
				t.Fatal("child did not crash")
			}
			snapshot, err := controller.Reload(context.Background())
			if err != nil {
				t.Fatalf("recover after process crash: %v", err)
			}
			if pre[phase] && snapshot.Revision != initial.Revision {
				t.Fatal("pre-CAS process crash changed active revision")
			}
			if !pre[phase] && snapshot.Revision == initial.Revision {
				t.Fatal("post-CAS process crash retained old revision")
			}
		})
	}
}

func TestOwnerPolicyPublicationCrashChild(t *testing.T) {
	root := os.Getenv("INGITDB_CRASH_ROOT")
	if root == "" {
		t.Skip("child only")
	}
	phase := os.Getenv("INGITDB_CRASH_PHASE")
	controller, _ := NewOwnerPolicyController(root)
	ownerPolicyPublicationHook = func(got string) error {
		if got == phase {
			os.Exit(91)
		}
		return nil
	}
	replacement := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: readers}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: deny, effect: deny, operations: [get]}]}]\n")}
	candidate := OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{replacement}}
	if phase == "snapshot_activated" {
		db, err := NewDatabase(root, nil)
		if err != nil {
			t.Fatal(err)
		}
		manager := db.(interface {
			PublishOwnerPolicyGeneration(context.Context, OwnerPolicyGeneration, string, string) (OwnerPolicyPublication, error)
		})
		_, _ = manager.PublishOwnerPolicyGeneration(context.Background(), candidate, os.Getenv("INGITDB_CRASH_EXPECTED"), "replacement")
	} else {
		_, _ = controller.Publish(context.Background(), candidate, os.Getenv("INGITDB_CRASH_EXPECTED"), "replacement")
	}
	t.Fatal("checkpoint not reached")
}

func runGitForGenerationTest(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func TestBuildGenerationBounds(t *testing.T) {
	policy := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: r, effect: allow, operations: [get]}]}]\n")}
	if _, _, _, err := buildGeneration(OwnerPolicyGeneration{Enabled: true, Database: "world", Realm: " bad ", Policies: []OwnerPolicyDocument{policy}}); err == nil {
		t.Fatal("invalid realm accepted")
	}
	many := make([]OwnerPolicyDocument, 101)
	for i := range many {
		many[i] = policy
	}
	if _, _, _, err := buildGeneration(OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: many}); err == nil {
		t.Fatal("policy count limit not enforced")
	}
}

func TestUnreferencedCompleteGenerationDoesNotBlockRetry(t *testing.T) {
	root := t.TempDir()
	runGitForGenerationTest(t, root, "init")
	runGitForGenerationTest(t, root, "config", "user.name", "Test")
	runGitForGenerationTest(t, root, "config", "user.email", "test@example.invalid")
	runGitForGenerationTest(t, root, "commit", "--allow-empty", "-m", "initial")
	controller, _ := NewOwnerPolicyController(root)
	policy := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/*, rules: [{id: r, effect: allow, operations: [get]}]}]\n")}
	ownerPolicyPublicationHook = func(phase string) error {
		if phase == "generation_renamed" {
			return errors.New("stop")
		}
		return nil
	}
	_, err := controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{policy}}, "", "first")
	if err == nil {
		t.Fatal("failure not injected")
	}
	ownerPolicyPublicationHook = func(string) error { return nil }
	defer func() { ownerPolicyPublicationHook = func(string) error { return nil } }()
	if _, err := controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{policy}}, "", "retry"); err != nil {
		t.Fatalf("validated unreferenced generation blocked retry: %v", err)
	}
}

func TestFacadePostCommitErrorInstallsAuthoritativeSnapshot(t *testing.T) {
	root := t.TempDir()
	runGitForGenerationTest(t, root, "init")
	runGitForGenerationTest(t, root, "config", "user.name", "Test")
	runGitForGenerationTest(t, root, "config", "user.email", "test@example.invalid")
	runGitForGenerationTest(t, root, "commit", "--allow-empty", "-m", "initial")
	controller, _ := NewOwnerPolicyController(root)
	allow := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/one, rules: [{id: r, effect: allow, operations: [get]}]}]\n")}
	initial, err := controller.Publish(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{allow}}, "", "initial")
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := db.(interface {
		PublishOwnerPolicyGeneration(context.Context, OwnerPolicyGeneration, string, string) (OwnerPolicyPublication, error)
	})
	deny := OwnerPolicyDocument{YAML: []byte("apiVersion: dtql.org/access/v1\nkind: AccessPolicy\nmetadata: {name: p}\ntarget: {database: world}\ncomposition: dalgo-hierarchical-v1\ndefault: deny\nscopes: [{path: /x/one, rules: [{id: d, effect: deny, operations: [get]}]}]\n")}
	ownerPolicyPublicationHook = func(phase string) error {
		if phase == "head_committed" {
			return errors.New("response lost")
		}
		return nil
	}
	defer func() { ownerPolicyPublicationHook = func(string) error { return nil } }()
	if _, err := manager.PublishOwnerPolicyGeneration(context.Background(), OwnerPolicyGeneration{Enabled: true, Database: "world", Policies: []OwnerPolicyDocument{deny}}, initial.Revision, "deny"); err == nil {
		t.Fatal("post-commit error missing")
	}
	rec := record.NewRecordWithData(record.NewKeyWithID("x", "one"), map[string]any{})
	if err := db.Get(context.Background(), rec); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("old permissive snapshot remained after known commit: %v", err)
	}
}
