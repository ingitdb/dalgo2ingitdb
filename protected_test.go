package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/ddl"
	"github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

func setupProtectedCountries(t *testing.T) (dal.DB, *access.EnforcementCoordinator, string) {
	t.Helper()
	root := t.TempDir()
	legacy, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	modifier, _ := dal.As[ddl.SchemaModifier](legacy)
	err = modifier.CreateCollection(context.Background(), dbschema.CollectionDef{Name: "countries", Fields: []dbschema.FieldDef{
		{Name: "name", Type: dbschema.String}, {Name: "ownerID", Type: dbschema.String}, {Name: "secret", Type: dbschema.String},
	}})
	if err != nil {
		t.Fatal(err)
	}
	registerRootCollection(t, root, "countries", "countries")
	writeYAMLRecord(t, root, "countries", "one", "name: One\nownerID: u1\nsecret: alpha\n")
	writeYAMLRecord(t, root, "countries", "two", "name: Two\nownerID: u2\nsecret: beta\n")
	dir := filepath.Join(root, ".ingitdb", "access")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "enabled: true\ndatabase: protected\npolicies: [owner.yaml]\n"
	policy := `apiVersion: dtql.org/access/v1
kind: AccessPolicy
metadata: {name: owner}
target: {database: protected}
composition: dalgo-hierarchical-v1
default: deny
scopes:
  - path: /countries/*
    rules:
      - id: own-name
        effect: allow
        operations: [update]
        fields: [name]
        where:
          op: "=="
          left: {field: ownerID}
          right: {param: currentUser}
      - id: read-name
        effect: allow
        operations: [get]
        fields: [name]
`
	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "owner.yaml"), []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader(), dalgo2ingitdb.WithProtectedProfile())
	if err != nil {
		t.Fatal(err)
	}
	factory, ok := db.(dalgo2ingitdb.ProtectedAccessConfigurer)
	if !ok {
		t.Fatal("missing protected factory")
	}
	secured, coordinator, err := factory.ConfigureProtectedAccess()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := secured.(dalgo2ingitdb.ProtectedAccessConfigurer); ok {
		t.Fatal("configured facade permits mandatory-layer reconfiguration")
	}
	return secured, coordinator, root
}

func TestProtectedProfile_WriteOnlyConditionalUpdateAndDeniedBatchRollback(t *testing.T) {
	db, _, root := setupProtectedCountries(t)
	ctx := access.WithCurrentUser(context.Background(), "u1")
	keyOne := record.NewKeyWithID("countries", "one")
	writer, ok := dal.As[dal.WriteSession](db)
	if !ok {
		t.Fatal("protected database lost write session")
	}
	if err := writer.Update(ctx, keyOne, []update.Update{update.ByFieldName("name", "Changed")}); err != nil {
		t.Fatalf("write-only update: %v", err)
	}
	readDenied := record.NewRecordWithData(keyOne, map[string]any{})
	if err := db.Get(ctx, readDenied); err != nil {
		t.Fatalf("field-filtered protected get=%v", err)
	}
	if data := readDenied.Data().(map[string]any); data["name"] != "Changed" || data["secret"] != nil {
		t.Fatalf("protected read leaked fields: %#v", data)
	}
	keys := []*record.Key{keyOne, record.NewKeyWithID("countries", "two")}
	if err := writer.UpdateMulti(ctx, keys, []update.Update{update.ByFieldName("name", "Batch")}); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("batch=%v, want denied", err)
	}
	if err := os.RemoveAll(filepath.Join(root, ".ingitdb", "access")); err != nil {
		t.Fatal(err)
	}
	legacy, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader())
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"one": "Changed", "two": "Two"} {
		rec := record.NewRecordWithData(record.NewKeyWithID("countries", key), map[string]any{})
		if err := legacy.Get(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
		if got := rec.Data().(map[string]any)["name"]; got != want {
			t.Fatalf("%s name=%v want %s", key, got, want)
		}
	}
}

func TestProtectedProfile_RejectsDynamicTransactionBeforeCallback(t *testing.T) {
	db, _, _ := setupProtectedCountries(t)
	called := false
	err := db.RunReadwriteTransaction(context.Background(), func(context.Context, dal.ReadwriteTransaction) error { called = true; return nil })
	if err == nil {
		t.Fatal("dynamic transaction unexpectedly supported")
	}
	if called {
		t.Fatal("unsupported callback was invoked")
	}
}

func protectedNameRevision(t *testing.T, coordinator *access.EnforcementCoordinator, ctx context.Context, key *record.Key) string {
	t.Helper()
	op, err := access.NewProtectedEvidenceRead("read", access.Get, key, [][]string{{"name"}})
	if err != nil {
		t.Fatal(err)
	}
	var revision string
	err = coordinator.WithinInspection(ctx, []access.ProtectedOperation{op}, func(session access.InspectionSession) error {
		facts, err := session.Evidence(ctx)
		if err != nil {
			return err
		}
		revision = facts[0].DataRevision
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func TestProtectedProfile_RevisionsDetectHiddenChangesAndDryRunIsStable(t *testing.T) {
	_, coordinator, root := setupProtectedCountries(t)
	ctx := access.WithCurrentUser(context.Background(), "u1")
	key := record.NewKeyWithID("countries", "one")
	path := filepath.Join(root, "countries", "$records", "one.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	revision := protectedNameRevision(t, coordinator, ctx, key)
	if again := protectedNameRevision(t, coordinator, ctx, key); again != revision {
		t.Fatalf("stable revision changed: %s != %s", again, revision)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("inspection mutated storage")
	}
	if err := os.WriteFile(path, []byte("name: One\nownerID: u1\nsecret: changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	op, err := access.NewProtectedUpdate("stale", key, []update.Update{update.ByFieldName("name", "Rejected")}, revision)
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(session access.ExecutionSession) error { _, err := session.Execute(ctx); return err })
	if !errors.Is(err, access.ErrDataRevisionConflict) {
		t.Fatalf("stale update=%v, want revision conflict", err)
	}
}

func TestProtectedProfile_CallbackErrorAfterExecuteRollsBack(t *testing.T) {
	_, coordinator, root := setupProtectedCountries(t)
	ctx := access.WithCurrentUser(context.Background(), "u1")
	key := record.NewKeyWithID("countries", "one")
	path := filepath.Join(root, "countries", "$records", "one.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	op, err := access.NewProtectedUpdate("rollback", key, []update.Update{update.ByFieldName("name", "Temporary")}, "")
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("after execute")
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(session access.ExecutionSession) error {
		if _, err := session.Execute(ctx); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("execution=%v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("callback error did not rollback")
	}
}

func TestProtectedProfile_LockWaitHonorsCancellation(t *testing.T) {
	_, coordinator, _ := setupProtectedCountries(t)
	ctx := access.WithCurrentUser(context.Background(), "u1")
	key := record.NewKeyWithID("countries", "one")
	op, err := access.NewProtectedUpdate("hold", key, []update.Update{update.ByFieldName("name", "Held")}, "")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(session access.ExecutionSession) error {
			close(entered)
			<-release
			_, err := session.Execute(ctx)
			return err
		})
	}()
	<-entered
	waitCtx, cancel := context.WithCancel(ctx)
	cancel()
	read, err := access.NewProtectedRead("blocked", access.Get, key)
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.WithinInspection(waitCtx, []access.ProtectedOperation{read}, func(access.InspectionSession) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lock wait=%v, want canceled", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
